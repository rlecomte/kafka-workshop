# Outils de maintenance du workshop

Cette branche ne contient que les scripts qui génèrent les branches du
workshop. Elle n'est pas destinée aux participants.

## Principe

Les sources de vérité sont :

- `chapitre/ch00`, le squelette du chapitre 0, écrit à la main ;
- `solution/chNN`, la solution de chaque chapitre (code, tests, et, pour
  `solution/ch08`, les énoncés, le README, `docker-compose.yml` et `scripts/`).

`outils/build-chain.sh` reconstruit toute la chaîne
`chapitre/ch00 → solution/ch00 → chapitre/ch01 → ... → solution/ch08` :

- chaque `solution/chNN` reprend l'arbre de `solution/chNN`, avec les
  énoncés de `solution/ch08` ;
- chaque `chapitre/chNN` (N ≥ 1) est généré à partir de `solution/chNN` par
  `outils/strip.py`, qui remplace le corps des fonctions du chapitre par
  `todo!("ch.N : nom")`, retire les API du chapitre de `SUPPORTED_APIS` et
  ajoute les `#![allow(...)]` du squelette.

La liste des fonctions de chaque chapitre est dans `build-chain.sh` : elle
doit correspondre aux sections « À faire » des énoncés.

## Modifier un chapitre

```bash
git checkout solution/ch05
# ... modifier, committer ...
# reporter la modification dans les solutions suivantes si besoin
# (cherry-pick sur solution/ch06, solution/ch07, solution/ch08)
git show outils:outils/build-chain.sh > /tmp/build-chain.sh
git show outils:outils/strip.py > /tmp/strip.py
bash /tmp/build-chain.sh
git checkout main && git reset --hard chapitre/ch00
```

Puis vérifier : chaque `chapitre/chNN` compile (`cargo test --no-run`) et
chaque `solution/chNN` passe ses tests (`cargo test`, avec
`docker compose up -d etcd` à partir du chapitre 6).
