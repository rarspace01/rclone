// Package squashfs implements a squashfs archiver for the archive backend
package squashfs

// FIXME this will perform poorly for unpacking as the VFS Reader is bad
// at multiple streams - need cache mode setting?

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdfs "io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/archive/archiver"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/squashfs"
)

func init() {
	archiver.Register(archiver.Archiver{
		New:       New,
		Extension: ".sqfs",
	})
}

// Fs represents a wrapped fs.Fs
type Fs struct {
	f           fs.Fs
	wrapper     fs.Fs
	name        string
	features    *fs.Features // optional features
	vfs         *vfs.VFS
	rd          *squashfs.Reader        // interface to the squashfs
	fh          vfs.Handle              // handle to the open squashfs archive
	node        vfs.Node                // squashfs file object - set if reading
	remote      string                  // remote of the squashfs file object
	prefix      string                  // position for objects
	prefixSlash string                  // position for objects with a slash on
	root        string                  // position to read from within the archive
	dt          dirtree.DirTree         // read from zipfile
	dirCacheMu  sync.Mutex              // protects the directory cache
	dirCache    map[string]*squashfs.FS // directory cache
}

// New constructs an Fs from the (wrappedFs, remote) with the objects
// prefix with prefix and rooted at root
func New(ctx context.Context, wrappedFs fs.Fs, remote, prefix, root string) (fs.Fs, error) {
	// FIXME vfs cache?
	// FIXME could factor out ReadFileHandle and just use that rather than the full VFS
	fs.Debugf(nil, "Squashfs: New: remote=%q, prefix=%q, root=%q", remote, prefix, root)
	VFS := vfs.New(wrappedFs, nil)
	node, err := VFS.Stat(remote)
	if err != nil {
		return nil, fmt.Errorf("failed to find %q archive: %w", remote, err)
	}

	fh, err := node.Open(os.O_RDONLY)
	if err != nil {
		return nil, fmt.Errorf("failed to open squashfs archive: %w", err)
	}

	rd, err := squashfs.NewReader(fh)
	if err != nil {
		return nil, fmt.Errorf("squashfs reader creation failed: %w", err)
	}

	f := &Fs{
		f:           wrappedFs,
		name:        path.Join(fs.ConfigString(wrappedFs), remote),
		vfs:         VFS,
		node:        node,
		rd:          rd,
		fh:          fh,
		remote:      remote,
		root:        root,
		prefix:      prefix,
		prefixSlash: prefix + "/",
		dirCache: map[string]*squashfs.FS{
			"": rd.FS,
		},
	}
	if prefix == "" {
		f.prefixSlash = ""
	}

	singleObject := false

	// Find the directory the root points to
	if root != "" {
		dir, err := f.findDir(root)
		// fs.Debugf(nil, "root=%s, dir=%#v, err=%v", root, dir, err)
		if err != nil {
			if err != fs.ErrorIsFile {
				return nil, err
			}
			// If it pointed to a file, find the directory above
			dir, _, err = f.findParent(root)
			// fs.Debugf(nil, "FILE root=%s, dir=%#v, err=%v", root, dir, err)
			if err != nil {
				return nil, err
			}
			f.root = path.Dir(root)
			if f.root == "." || f.root == "/" {
				f.root = ""
			}
			singleObject = true
		}
		// Reset the root directory
		f.dirCache = map[string]*squashfs.FS{
			"": dir,
		}
	}

	// FIXME
	// the features here are ones we could support, and they are
	// ANDed with the ones from wrappedFs
	//
	// FIXME some of these need to be forced on - CanHaveEmptyDirectories
	f.features = (&fs.Features{
		CaseInsensitive:         false,
		DuplicateFiles:          false,
		ReadMimeType:            false, // MimeTypes not supported with gsquashfs
		WriteMimeType:           false,
		BucketBased:             false,
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f).Mask(ctx, wrappedFs).WrapsFs(f, wrappedFs)

	if singleObject {
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("Squashfs %q", f.name)
}

// Find the directory at dirPath
//
// Call with dirCacheMu held
func (f *Fs) _findDir(dirPath string) (*squashfs.FS, error) {
	// fs.Debugf(nil, "dirPath = %q", dirPath)
	dir, found := f.dirCache[dirPath]
	if found {
		return dir, nil
	}
	var leaf string
	for {
		i := strings.LastIndexByte(dirPath, '/')
		if i < 0 {
			dirPath, leaf = "", dirPath
			break
		}
		dirPath, leaf = dirPath[:i], dirPath[i+1:]
		if leaf != "" {
			break
		}
	}
	parent, err := f._findDir(dirPath)
	if err != nil {
		return nil, err
	}
	stdDir, err := parent.Sub(leaf)
	if err != nil {
		// We get this error if leaf points to a file
		if errors.Is(err, stdfs.ErrNotExist) {
			return nil, fs.ErrorIsFile
		}
		return nil, err
	}
	dir, ok := stdDir.(*squashfs.FS)
	if !ok {
		return nil, fmt.Errorf("internal error: dir=%#v wasn't a *squashfs.FS", stdDir)
	}
	f.dirCache[path.Join(dirPath, leaf)] = dir
	return dir, nil
}

// Find the directory at dirPath
func (f *Fs) findDir(dirPath string) (*squashfs.FS, error) {
	f.dirCacheMu.Lock()
	defer f.dirCacheMu.Unlock()
	dirPath = strings.Trim(dirPath, "/")
	return f._findDir(dirPath)
}

// findParent finds the dir this file lives in
func (f *Fs) findParent(name string) (parent *squashfs.FS, leaf string, err error) {
	name = strings.Trim(name, "/")
	parentPath, leaf := path.Split(name)
	parent, err = f.findDir(parentPath)
	if err != nil {
		return nil, "", err
	}
	return parent, leaf, nil
}

// Strip the prefix from remote
func (f *Fs) stripPrefix(remote string) (string, error) {
	remote = strings.Trim(remote, "/")
	if remote == f.prefix {
		remote = ""
	} else if !strings.HasPrefix(remote, f.prefixSlash) {
		return "", fmt.Errorf("internal error: %q doesn't start with prefix %q", remote, f.prefixSlash)
	} else {
		remote = remote[len(f.prefixSlash):]
	}
	return remote, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	defer log.Trace(f, "dir=%q", dir)("entries=%v, err=%v", &entries, &err)

	dir, err = f.stripPrefix(dir)
	if err != nil {
		return nil, err
	}

	dirFS, err := f.findDir(dir)
	if err != nil {
		return nil, err
	}

	items, err := dirFS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("read squashfs: couldn't read directory: %w", err)
	}

	entries = make(fs.DirEntries, 0, len(items))
	for _, item := range items {
		// fs.Debugf(item.Name(), "entry = %#v", item)
		var entry fs.DirEntry
		var remote = f.prefixSlash + path.Join(dir, item.Name())
		fi, err := item.Info()
		if err != nil {
			return nil, fmt.Errorf("error reading item %q: %q", item.Name(), err)
		}
		if item.IsDir() {
			entry = fs.NewDir(remote, fi.ModTime())
		} else {
			entry = &Object{
				fs:      f,
				remote:  remote,
				size:    fi.Size(),
				modTime: fi.ModTime(),
			}
		}
		entries = append(entries, entry)
	}

	// fs.Debugf(f, "dir=%q, entries=%v", dir, entries)
	return entries, nil
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (o fs.Object, err error) {
	defer log.Trace(f, "remote=%q", remote)("obj=%v, err=%v", &o, &err)
	if f.dt == nil {
		return nil, fs.ErrorObjectNotFound
	}
	_, entry := f.dt.Find(remote)
	if entry == nil {
		return nil, fs.ErrorObjectNotFound
	}
	o, ok := entry.(*Object)
	if !ok {
		return nil, fs.ErrorNotAFile
	}
	return o, nil
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return vfs.EROFS
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return vfs.EROFS
}

// Put in to the remote path with the modTime given of the given size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (o fs.Object, err error) {
	return nil, vfs.EROFS
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// UnWrap returns the Fs that this Fs is wrapping
func (f *Fs) UnWrap() fs.Fs {
	return f.f
}

// WrapFs returns the Fs that is wrapping this Fs
func (f *Fs) WrapFs() fs.Fs {
	return f.wrapper
}

// SetWrapper sets the Fs that is wrapping this Fs
func (f *Fs) SetWrapper(wrapper fs.Fs) {
	f.wrapper = wrapper
}

// Object describes an object to be read from the raw squashfs file
type Object struct {
	fs      *Fs
	remote  string
	size    int64
	modTime time.Time
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Remote()
}

// Turn a squashfs path into a full path for the parent Fs
// func (o *Object) path(remote string) string {
// 	return path.Join(o.fs.prefix, remote)
// }

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return vfs.EROFS
}

// Storable raturns a boolean indicating if this object is storable
func (o *Object) Storable() bool {
	return true
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, ht hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (rc io.ReadCloser, err error) {
	var offset, limit int64 = 0, -1
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		case *fs.RangeOption:
			offset, limit = x.Decode(o.Size())
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}

	remote, err := o.fs.stripPrefix(o.remote)
	if err != nil {
		return nil, err
	}

	dir, leaf, err := o.fs.findParent(remote)
	if err != nil {
		return nil, err
	}

	rc, err = dir.Open(leaf)
	if err != nil {
		return nil, err
	}

	// FIXME can we seek instead?
	// cast to io.Seeker

	// discard data from start as necessary
	if offset > 0 {
		_, err = io.CopyN(io.Discard, rc, offset)
		if err != nil {
			return nil, err
		}
	}
	// If limited then don't return everything
	if limit >= 0 {
		return readers.NewLimitedReadCloser(rc, limit-offset), nil
	}

	return rc, nil
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return vfs.EROFS
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return vfs.EROFS
}

// Check the interfaces are satisfied
var (
	_ fs.Fs        = (*Fs)(nil)
	_ fs.UnWrapper = (*Fs)(nil)
	//	_ fs.Abouter         = (*Fs)(nil) - FIXME can implemnet
	_ fs.Wrapper = (*Fs)(nil)
	_ fs.Object  = (*Object)(nil)
)
