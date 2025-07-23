#!/bin/bash

COMMIT_MSG=${1:-"auto commit"}

# git 작업
git status
git add -A
git commit -m "$COMMIT_MSG"
git push
