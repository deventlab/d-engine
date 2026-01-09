# Contributing to d-engine

Thank you for your interest in d-engine! We welcome contributions that align with our vision.

## Philosophy: Small, Sharp, Focused

d-engine is a **coordination engine, not a kitchen sink**. We follow these principles:

- **Simple over complex** - single-threaded event loop beats multi-threaded chaos
- **20/80 rule** - solve 80% of real-world problems with 20% of features
- **User-driven, not experiment-driven** - features come from production pain, not wishful thinking
- **Small and beautiful, not large and complete** - like a screw that does one thing perfectly

If your contribution adds complexity without solving a real problem for most users, it will be declined.

---

## Before You Contribute

### For Feature Ideas: Open an Issue First

**All feature PRs require prior discussion in an issue.**

We're building for real production use cases, not theoretical scenarios. When proposing features:

1. **Describe the real-world problem** - "Our company needs X because..." not "It would be cool if..."
2. **Show it's a common need** - Does this solve problems for 80% of users or just your edge case?
3. **Explain why existing features don't work** - What have you tried? Why isn't it sufficient?

Features that don't meet this bar will be deferred or declined, even if the code is good.

### For Bug Fixes: PRs Welcome Directly

Bug fixes with reproduction tests can be submitted directly - no prior issue needed.

---

## Current Focus (Pre-v1.0)

We're in **active development** based on real user feedback. Our priorities evolve as we learn from production deployments.

**Core stability always comes first:**

- Raft protocol correctness under all failure scenarios
- Runtime stability and performance
- Production-grade observability

**We're open to (but not actively working on):**

- Storage backend extensions for cloud-native deployments
- Integration patterns for container orchestration platforms
- Performance optimizations backed by real benchmarks

**Out of scope for v1.0:**

- Alternative consensus protocols (Raft is our foundation)
- Features that violate our simplicity principle
- "Nice to have" additions without clear production use cases

**Roadmap is flexible** - it adapts to real user needs, not preset milestones.

---

## Development Workflow

### Quick Setup

```bash
git clone https://github.com/deventlab/d-engine.git
cd d-engine
make test-all  # Runs fmt, clippy, and all tests
```

### Branch Naming

- Features: `feature/<ticket-number>_<short-description>`
- Bug fixes: `bug/<ticket-number>_<short-description>`
- Target `develop` branch for all PRs

### Code Quality

- **Format & Link**: `make check` (required)
- **Test**: `make test-all` must pass

---

## Pull Request Guidelines

### Before Submitting

- [ ] Feature approved in issue (if applicable)
- [ ] `make test-all` passes locally
- [ ] Added tests for new code
- [ ] Updated docs if API changed
- [ ] Commits squashed to 1-2 logical units

### What We Look For

✅ **Good PRs:**

- Fix bugs with reproduction tests
- Improve documentation clarity
- Add test coverage
- Performance improvements with benchmark proof

❌ **Common Issues:**

- No prior discussion for features
- Solves only your specific edge case
- Adds complexity without clear benefit
- Missing tests or breaks existing ones

### PR Size

- **Preferred**: < 300 lines
- **Large PRs** (> 500 lines) will be asked to split
- Smaller PRs get faster reviews

---

## Review Process

This is a **single-maintainer project** with limited bandwidth:

- Small PRs (< 100 lines): ~1-2 weeks
- Large PRs: ~2-4 weeks
- Active development takes priority

**Important**: Even well-written code may be declined if it conflicts with project direction. This keeps d-engine focused and maintainable.

---

## How to Get Priorities Right

**Best ways to contribute:**

1. **Report bugs with clear reproduction steps**
2. **Share your production use case** - "We're using d-engine for X and hit Y problem"
3. **Improve documentation** - where did you get confused?
4. **Add test coverage** - especially for edge cases

**Less effective:**

- "This would be cool" features
- Premature optimizations
- Copying features from other systems without justification

---

## Questions?

- **Bug reports**: Use [Bug Report template](.github/ISSUE_TEMPLATE/bug_report.md)
- **Feature ideas**: Use [Feature Request template](.github/ISSUE_TEMPLATE/feature_request.md)
- **General questions**: Open a [Discussion](https://github.com/deventlab/d-engine/discussions)

---

## License

By contributing, you agree to license your work under:

- [MIT License](LICENSE-MIT) or [Apache License 2.0](LICENSE-APACHE)

---

**Remember**: We value **focus over features**. d-engine is a screw, not a Swiss Army knife.
