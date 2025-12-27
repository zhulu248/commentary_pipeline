# Syncing your local copy and running smoke tests

## What `git pull` does
- `git pull` downloads new commits from the remote Git repository and merges them into your current branch.
- Use it when you already cloned the repository and just want the latest changes.

## Fresh clone (if you do not have the repo locally)
```bash
git clone <repo-url> commentary_pipeline
cd commentary_pipeline
```
Replace `<repo-url>` with the HTTPS/SSH URL of your fork or the upstream repository.

## Update an existing clone
```bash
cd commentary_pipeline
git pull
```
This fetches the latest commits from the tracked remote and merges them into your current branch.

### Verify you are on the newest commit
From inside the repo:
```bash
git status -sb           # should show a clean tree like: "## main"
git fetch origin         # update remote refs
git rev-parse HEAD       # your current commit hash
git rev-parse origin/main  # remote main hash
```
If the two hashes match, you are fully up to date. If they differ, rerun `git pull` and repeat the checks. Running outside of
OneDrive/Cloud Drive folders on Windows (e.g., `C:\Users\<you>\Documents\project\commentary_pipeline`) avoids sync quirks
that can hide updates.

## Quick smoke test (no network required)
Run the help text for the CPF converter to ensure the CLI entry point imports correctly:
```bash
python 01_crawl_convert/convert_webpage_to_cpf.py --help
```
You should see an `Example:` line near the top like:
```text
Example:
  python 01_crawl_convert/convert_webpage_to_cpf.py "https://www.monergism.com/second-coming-our-lord-and-millennium" --type article -o output.cpf.txt
```

**If you do not see the example line:**
1) Re-run a fresh pull to ensure you are synced: `git pull`
2) Re-run the help command but limit output to the top lines to make the example obvious:
   - macOS/Linux: `python 01_crawl_convert/convert_webpage_to_cpf.py --help | head -n 20`
   - Windows PowerShell: `python 01_crawl_convert/convert_webpage_to_cpf.py --help | Select-Object -First 20`
3) If the example is still missing, check that you are outside OneDrive/Cloud folders and verify the hashes in the section above.

Once the example shows, your Python environment can start the converter; you can then run it with a real URL when ready.

## Optional end-to-end sample (requires internet access)
```bash
python 01_crawl_convert/convert_webpage_to_cpf.py "<https-url>" --type article -o output.cpf.txt --fetch auto
```
Tips:
- Always include the full `https://` prefix to avoid the `MissingSchema` error.
- If a site blocks regular requests, re-run with `--fetch playwright` after installing Playwright and a browser.

## Where to find the updated files
After a successful `git pull`, the latest committed files—including scripts and docs—are in your local checkout. You can open them directly in VS Code or another editor to review changes.
