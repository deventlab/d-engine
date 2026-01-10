---
name: Feature Request
about: Propose a feature based on real production needs
title: "[Feature] "
labels: "enhancement"
assignees: ""
---

## ⚠️ Read First

d-engine follows the **20/80 rule** - we build features that solve 80% of users' problems, not edge cases.

**Before submitting:**

- [ ] This solves a **real production problem** (not "it would be nice")
- [ ] I've checked existing features don't solve this

Features that don't meet this bar will be deferred or declined.

---

## Production Context

**What production problem does this solve?**  
(Be specific: "Our company runs d-engine for X and hits Y problem daily")

**Who is affected?**

- [ ] Most embedded mode users
- [ ] Most standalone mode users
- [ ] Specific industry/scale (describe below)

**Current workaround:**  
(What are you doing now? Why is it insufficient?)

---

## Proposed Solution

**Your solution:**

**Why is this the simplest approach?**  
(d-engine values simplicity - explain why this doesn't bloat the API)

---

## Impact vs Complexity

**Benefits:**

- Performance impact: (quantify if possible)
- User pain reduced: (describe)

**Complexity added:**

- [ ] New public APIs
- [ ] More config options
- [ ] Additional dependencies
- [ ] Increased testing surface

---

## Additional Context

- Benchmarks/measurements (if performance-related)
- Similar features in etcd/consul (if applicable)
- Your willingness to contribute code
