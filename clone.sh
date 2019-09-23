#!/bin/bash
cd "$(dirname "$0")"

if [ -z "$MUNICIPIOS_MIRROR_DIR" ]; then
  echo "Debe exisistir una variable de entorno llamada MUNICIPIOS_MIRROR_DIR que apunte a un directorio"
  exit 1
fi
if [ -z "$MUNICIPIOS_MIRROR_GIT" ]; then
  echo "Debe exisistir una variable de entorno llamada MUNICIPIOS_MIRROR_GIT que apunte al repositorio git que va a hacer de mirror"
  exit 1
fi
MUNICIPIOS_ORIGIN_GIT=$(git config --get remote.origin.url)
if [ ! -d "$MUNICIPIOS_MIRROR_DIR" ]; then
  mkdir -p "$MUNICIPIOS_MIRROR_DIR"
fi
cd "$MUNICIPIOS_MIRROR_DIR"

if [ "$(ls -A "$MUNICIPIOS_MIRROR_DIR")" ]; then
     git clone --mirror "$MUNICIPIOS_ORIGIN_GIT" "$MUNICIPIOS_MIRROR_DIR"
fi

git pull
git push sync --mirror
