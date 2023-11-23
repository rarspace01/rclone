package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/spf13/cobra"
)

// Add remotes to the completions being built up
func addRemotes(toComplete string, completions []string) []string {
	remotes := config.FileSections()
	for _, remote := range remotes {
		remote += ":"
		if strings.HasPrefix(remote, toComplete) {
			completions = append(completions, remote)
		}
	}
	return completions
}

// Add local files to the completions being built up
func addLocalFiles(toComplete string, result cobra.ShellCompDirective, completions []string) (cobra.ShellCompDirective, []string) {
	path := filepath.Clean(toComplete)
	dir, file := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	if len(dir) > 0 && dir[0] != filepath.Separator && dir[0] != '/' {
		dir = strings.TrimRight(dir, string(filepath.Separator))
		dir = strings.TrimRight(dir, "/")
	}
	fi, err := os.Stat(toComplete)
	if err == nil {
		if fi.IsDir() {
			dir = toComplete
			file = ""
		}
	}
	fis, err := os.ReadDir(dir)
	if err != nil {
		return result, completions
	}
	for _, fi := range fis {
		name := fi.Name()
		if strings.HasPrefix(name, file) {
			path := filepath.Join(dir, name)
			if fi.IsDir() {
				path += string(filepath.Separator)
				result |= cobra.ShellCompDirectiveNoSpace
			}
			completions = append(completions, path)
		}
	}
	return result, completions
}

// Add remote files to the completions being built up
func addRemoteFiles(toComplete string, result cobra.ShellCompDirective, completions []string) (cobra.ShellCompDirective, []string) {
	ctx := context.Background()
	parent, _, err := fspath.Split(toComplete)
	if err != nil {
		return result, completions
	}
	f, err := cache.Get(ctx, parent)
	if err == fs.ErrorIsFile {
		completions = append(completions, toComplete)
		return result, completions
	} else if err != nil {
		return result, completions
	}
	fis, err := f.List(ctx, "")
	if err != nil {
		return result, completions
	}
	for _, fi := range fis {
		remote := fi.Remote()
		path := parent + remote
		fs.Debugf(fi, "path = %q", path)
		if strings.HasPrefix(path, toComplete) {
			if _, ok := fi.(fs.Directory); ok {
				path += "/"
				result |= cobra.ShellCompDirectiveNoSpace
			}
			completions = append(completions, path)
		}
	}
	return result, completions
}

// do command completion
//
// This is called by the command completion scripts using a hidden __complete or __completeNoDesc commands.
func validArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	// Work around what I think is a bug in cobra's bash
	// completion which seems to be splitting the arguments on :
	// Or there is something I don't understand - ncw
	cobra.CompDebugln(fmt.Sprintf("args=%q toComplete=%q", args, toComplete), true)
	fixBug := -1
	args = append(args, toComplete)
	colonArg := -1
	for i, arg := range args {
		if arg == ":" {
			colonArg = i
		}
	}
	if colonArg > 0 {
		newToComplete := strings.Join(args[colonArg-1:], "")
		fixBug = len(newToComplete) - len(toComplete)
		toComplete = newToComplete
	}
	cobra.CompDebugln(fmt.Sprintf("args=%q toComplete=%q", args, toComplete), true)
	result := cobra.ShellCompDirectiveDefault
	completions := []string{}

	// See whether we have a valid remote yet
	_, err := fspath.Parse(toComplete)
	parseOK := err == nil
	hasColon := strings.ContainsRune(toComplete, ':')
	validRemote := parseOK && hasColon

	// Add remotes for completion
	if !validRemote {
		completions = addRemotes(toComplete, completions)
	}

	// Add local files for completion
	if !validRemote {
		result, completions = addLocalFiles(toComplete, result, completions)
	}

	// Add remote files for completion
	if validRemote {
		result, completions = addRemoteFiles(toComplete, result, completions)
	}

	// If using bug workaround, adjust completions to start with :
	if fixBug >= 0 {
		for i := range completions {
			if len(completions[i]) >= fixBug {
				completions[i] = completions[i][fixBug:]
			}
		}
	}

	return completions, result
}
