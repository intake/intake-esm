---
name: Bug report
about: Create a report to help us improve
title: ""
labels: ""
assignees: ""
---

<!--

Thanks for submitting an issue!

-->

Here's a quick checklist in what to include:

- [ ] Include a detailed description of the bug or suggestion
- [ ] Output of `intake_esm.show_versions()`
- [ ] Minimal, self-contained copy-pastable example that generates the issue if possible. Please be concise with code posted. See guidelines below on how to provide a good bug report:

  - [Minimal Complete Verifiable Examples](https://stackoverflow.com/help/mcve)
  - [Craft Minimal Bug Reports](http://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports)

    Bug reports that follow these guidelines are easier to diagnose,
    and so are often handled much more quickly.

### Description

```
Describe what you were trying to get done.
Tell us what happened, what went wrong, and what you expected to happen.
```

### What I Did

```
Paste the command(s) you ran and the output.
If there was a crash, please include the traceback here.
```

### Version information: output of `intake_esm.show_versions()`

<details>

Paste the output of `intake_esm.show_versions()` here:

```python
import intake_esm

intake_esm.show_versions()
```

</details>
