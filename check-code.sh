#!/usr/bin/env bash
#
# Run Python checkers and formatters.

case $# in
  0)
    args="."
    ;;
  *)
    args="$@"
    ;;
esac

die() {
  echo "$0: $1" >&2
  exit 1
}

for i in $args
do
  echo "Checking types in $i"
  if [ "$i" = "." ]
  then
    pyright sqlshell || die "pyright failed"
  else
    pyright $i || die "pyright failed"
  fi

  echo "pylint $i"
  if [ "$i" = "." ]
  then
    pylint sqlshell || die "pylint failed"
  else
    pylint $i || die "pylint failed"
  fi

  # pycheck is a personal tool in a private repo. Feel
  # free to comment this out.
  if [ "$i" = "." ]
  then
    echo "pycheck -cf " sqlshell/*.py
    pycheck -cf sqlshell/*.py || die "pycheck failed"
  else
    echo "pycheck -cf $i"
    pycheck -cf $i || die "pycheck failed"
  fi

  #echo "Sorting imports in $i"
  #isort $i

  #echo "Formatting $i with black"
  #black $i
done
