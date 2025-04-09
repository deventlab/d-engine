# Contributing to dengine

Thank you for considering contributing to dengine! We welcome all forms of contributions.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/<your-username>/dengine.git`
3. Create a new branch from `develop`:
   - Features: `feature/<short-description>`
   - Bug fixes: `bug/<short-description>`
4. Make your changes following the guidelines below
5. Submit a Pull Request to the `develop` branch

## Development Practices

### Code Style
- Follow Rust formatting standards: run `cargo fmt` before committing
- Adhere to Clippy recommendations: check with `cargo clippy --all-targets --all-features`
- Use idiomatic Rust patterns and proper documentation

### Testing
- Add unit tests for new functionality
- Integration tests for protocol-critical components
- Run tests locally: `cargo test --all-features`
- Maintain test coverage - PRs should not significantly decrease coverage

### Commit Messages
- Use present tense ("Add feature" not "Added feature")
- Keep first line under 50 characters
- Provide additional details in body when needed

### Pull Requests
- Target the `develop` branch
- Ensure CI checks pass (formatting, linting, tests)
- Update documentation if needed
- Include relevant test updates
- Describe changes clearly in the PR description

## CI/CD
Our CI pipeline ensures:
- Formatting check with `cargo fmt`
- Linting with Clippy
- Test coverage reporting via Codecov
- Basic protocol safety checks

Check the [CI configuration](.github/workflows/ci.yml) for details.

## License
By contributing, you agree to license your work under the project's dual license:
- MIT License
- Apache License 2.0

See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT) for full details.
