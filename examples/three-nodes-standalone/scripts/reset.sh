#!/bin/bash

# List of directories to check
dirs=("db" "logs"  )

# Loop through the list of directories
for dir in "${dirs[@]}"; do
  # Check if directory exists
  if [ ! -d "$dir" ]; then
    echo "Directory $dir does not exist. Creating..."
    mkdir "$dir"
  else
    echo "Directory $dir already exists."
  fi
done

rm -rf db/*
rm -rf logs/*
