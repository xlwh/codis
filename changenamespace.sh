#!/bin/bash

find ./ -name *.go -exec sed -i 's/github.com\/wandoulabs\/codis/github.com\/YongMan\/codis/g' {} \;
