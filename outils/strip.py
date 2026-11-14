#!/usr/bin/env python3
"""Génère le squelette d'un chapitre : remplace le corps de fonctions Rust par todo!().

Usage : strip.py <racine du dépôt> <chapitre> <fichier:fn1,fn2,...> ...
"""
import re
import sys


def skip_string(s, i):
    """i pointe sur le guillemet ouvrant d'une chaîne normale ; renvoie l'index après la fin."""
    i += 1
    while i < len(s):
        if s[i] == '\\':
            i += 2
            continue
        if s[i] == '"':
            return i + 1
        i += 1
    raise ValueError("chaîne non terminée")


def skip_raw_string(s, i):
    """i pointe sur le r de r#"..."# ; renvoie l'index après la fin, ou None."""
    j = i + 1
    hashes = 0
    while j < len(s) and s[j] == '#':
        hashes += 1
        j += 1
    if j >= len(s) or s[j] != '"':
        return None
    end = '"' + '#' * hashes
    k = s.index(end, j + 1)
    return k + len(end)


def find_matching_brace(s, open_index):
    """Renvoie l'index de l'accolade fermante qui correspond à s[open_index] == '{'."""
    depth = 0
    i = open_index
    while i < len(s):
        c = s[i]
        if s.startswith('//', i):
            i = s.index('\n', i)
            continue
        if s.startswith('/*', i):
            level, i = 1, i + 2
            while level:
                if s.startswith('/*', i):
                    level, i = level + 1, i + 2
                elif s.startswith('*/', i):
                    level, i = level - 1, i + 2
                else:
                    i += 1
            continue
        if c in 'rb' and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] == '_')):
            start = i + 1 if (c == 'b' and s.startswith('br', i)) else i
            if s[start] == 'r':
                end = skip_raw_string(s, start)
                if end is not None:
                    i = end
                    continue
            if c == 'b' and i + 1 < len(s) and s[i + 1] == '"':
                i = skip_string(s, i + 1)
                continue
        if c == '"':
            i = skip_string(s, i)
            continue
        if c == "'":
            # Littéral caractère ('x', '\n', '{') ou durée de vie ('a, 'static).
            if i + 1 < len(s) and s[i + 1] == '\\':
                i = s.index("'", i + 2) + 1
                continue
            if i + 2 < len(s) and s[i + 2] == "'":
                i += 3
                continue
            i += 1
            continue
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise ValueError("accolade non fermée")


def strip_functions(text, names, chapter):
    for name in names:
        pattern = re.compile(r'^([ \t]*)(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+' + re.escape(name) + r'\s*[<(]', re.M)
        matches = list(pattern.finditer(text))
        if not matches:
            raise SystemExit(f"fonction {name} introuvable")
        # De la fin vers le début, pour ne pas décaler les positions.
        for m in reversed(matches):
            indent = m.group(1)
            open_index = text.index('{', m.end())
            close_index = find_matching_brace(text, open_index)
            body = f'{{\n{indent}    todo!("ch.{chapter} : {name}")\n{indent}}}'
            text = text[:open_index] + body + text[close_index + 1:]
    return text


def remove_api_block(text, chapter):
    """Retire les lignes de SUPPORTED_APIS du chapitre (après `// ch.N`)."""
    lines = text.split('\n')
    out, skipping = [], False
    for line in lines:
        stripped = line.strip()
        if stripped == f'// ch.{chapter}' or stripped.startswith(f'// ch.{chapter} '):
            skipping = True
            out.append(line.replace(stripped, f'// ch.{chapter} : à compléter'))
            continue
        if skipping and (stripped.startswith('// ch.') or stripped == '];'):
            skipping = False
        if skipping:
            continue
        out.append(line)
    return '\n'.join(out)


LINTS = (
    "// Squelette du workshop : tant que les `todo!()` ne sont pas remplacés,\n"
    "// certains imports, paramètres et fonctions ne servent pas encore.\n"
    "#![allow(dead_code, unused_imports, unused_variables, unused_mut)]\n"
)


def add_lints(text):
    lines = text.split('\n')
    last_doc = max((i for i, l in enumerate(lines) if l.startswith('//!')), default=-1)
    lines.insert(last_doc + 1, '\n' + LINTS.rstrip('\n'))
    return '\n'.join(lines)


def main():
    root, chapter, specs = sys.argv[1], sys.argv[2], sys.argv[3:]
    for spec in specs:
        path, names = spec.split(':', 1)
        full = f'{root}/{path}'
        text = open(full).read()
        if names == '@apis':
            text = remove_api_block(text, chapter)
        elif names == '@lints':
            text = add_lints(text)
        else:
            text = strip_functions(text, [n for n in names.split(',') if n], chapter)
        open(full, 'w').write(text)


if __name__ == '__main__':
    main()
