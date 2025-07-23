#!/bin/bash
git config --global user.email "parkhk1991@gmail.com"
git config --global user.name "jaewabi"

COMMIT_MSG=${1:-"auto commit"}

# git 작업
git status
git add -A
git commit -m "$COMMIT_MSG"
git push
