#!/bin/bash
sed -E '/^ *--/d' |
tr '\n' ' ' |
sed -E 's/ +/ /g'
