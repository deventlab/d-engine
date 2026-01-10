#!/bin/bash

# Start SSH agent for container lifetime
eval $(ssh-agent -a /ssh-agent) > /dev/null

# Add SSH keys (silently)
ssh-add /root/.ssh/id_rsa > /dev/null 2>&1

# Execute main command
exec "$@"
