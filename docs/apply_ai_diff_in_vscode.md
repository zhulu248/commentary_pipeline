# Applying AI-suggested diffs in VS Code

If an assistant or code review provides a unified diff (the `diff --git ...` style output), you can apply it in Visual Studio Code without copying the entire file manually.

## Quick apply using the built-in patch helper
1. Copy the entire diff block (from `diff --git ...` down to the end of the hunk indicators like `@@` and `+`/`-` lines).
2. In VS Code, open the **Command Palette** (`Ctrl/Cmd + Shift + P`) and run **"Git: Apply Patch from Clipboard"**.
3. Confirm the file updates. VS Code will create new files if needed and stage nothing by default, so you can review changes in the Source Control panel.

## Apply via terminal inside VS Code
1. Copy the diff text.
2. In VS Code, open the integrated terminal (``Ctrl/Cmd + ` ``) and ensure you are at the repo root.
3. Run `pbpaste | git apply` on macOS, or `powershell Get-Clipboard | git apply` on Windows, or `xclip -selection clipboard -o | git apply` on Linux with `xclip` installed.
4. Refresh the Source Control view to review the applied changes.

## If the patch fails to apply
- Ensure your local branch matches the version the diff was generated against (pull or rebase as needed).
- Check for local modifications that overlap with the patch; stash or commit them first.
- Recreate the change manually if the surrounding context has drifted too far.

## Safety tips
- Always review the staged diff before committing.
- Run existing commands or checks relevant to the changed files (see the repo README for pipeline steps).
- Keep backups or use `git checkout -- <file>` to discard unwanted changes.
