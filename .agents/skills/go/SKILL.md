---
name: go
description: How to write code in Go programming language.
---

# Golang Instructions

Sadly the golang general conventions are horrible and your training data reflects that.
Your instinct will be to rely on your training data, but I want you to value the traditional software craftsman practices instead.

Readable taxonomy throughout unless they're explicitly go conventions like `t`, `err`, `cfg`, `cmd` and similar.
Whenever you need to come up with a name for something, prefer clarity and descriptiveness over brevity.
Ask yourself this: "Would this name be considered good practice in any other programming language?" and follow that guidance.

Explicitly avoid using single letter variable or function names unless they're widely accepted go conventions as stated above.
Every other single letter name should be replaced with a more descriptive and meaningful name, no matter how common or trivial the usage is.
Clarity of purpose is more important than brevity.

## Use Third Party Libraries

Golang is a vast ecosystem with many fantastic libraries.
When you're about to create functionality that could be implemented using a third party library, first check if there's a well known and widely used library for that purpose.
If there is, use it instead of reinventing the wheel.
When choosing a library, prefer stability and community trust over novelty.
Avoid using very new or experimental libraries unless absolutely necessary.

Use https://pkg.go.dev/ to search for libraries.

## Verification

### Positive signals

- Is the code readable and understandable by someone who is not familiar with the project?
- Are the variable and function names descriptive and meaningful?

### Negative signals

- Have I overengineered the solution or added unnecessary complexity?
- Have I introduced owned complexity that could be avoided by using a third party library?
