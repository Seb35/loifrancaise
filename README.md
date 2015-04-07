Loifrancaise
============

Bibliothèque pour manipuler les bases de données juridiques régissant le droit en France.

Plusieurs bases de données juridiques sont en [Open Data depuis juin 2014] [1], dont :
- LEGI : codes, lois et règlements consolidés
- KALI : conventions collectives nationales
- JORF : textes publiés au Journal officiel de la République française
- JORFSIMPLE : (?)
- CNIL : délibérations de la CNIL
- CONSTIT : décisions du Conseil constitutionnel

Voyez les conditions spécifiques de leur utilisation sur http://rip.journal-officiel.gouv.fr/.

Les fonctionnalités fournies sont :
- téléchargement des bases et gestion de leurs versions,
- lecture des bases pour effectuer des traitements à définir par l’utilisateur,
- divers utilitaires.

[1]: https://www.etalab.gouv.fr/les-bases-legi-kali-et-circulaires-sont-disponibles-en-open-data-sur-data-gouv-fr-sous-licence-ouverte

Développement
-------------

Cette bibliothèque est issue du projet [Archéo Lex] [2] qui vise à reformer dans un format texte ([Markdown] [3] plus précisément) les différentes versions en vigueur successivement des codes de loi français pour ensuite versionner ces différents versions sous le format _de facto_ standard de gestion de versions [Git] [4]. Les premières versions de cette bibliothèques correspondent donc uniquement à une légère généralisation des fonctionnalités requises par Archéo Lex.

Toutefois, il est possible d’ajouter toute fonctionnalité qui utiliserait ces bases juridiques (voire d’autres tant qu’elles concernent la loi française) et qui profiteraient à plusieurs utilisateurs. Par exemple, il serait probablement intéressant qu’une base de données relationnelle (SQL) puisse être construite à partir de ces bases dans le but de requêter facilement dessus. Autre exemple facilité par le précédent : extraire des statistiques.

[2]: https://archeo-lex.fr/
[3]: http://daringfireball.net/projects/markdown/
[4]: http://www.git-scm.org/

Licence
-------

Ce programme est sous licence [WTFPL 2.0] [5] avec clause de non-garantie. Voir le fichier LICENSE pour les détails.

[5]: http://www.wtfpl.net/

Contact
-------

Des listes de discussions sont disponibles sur le projet Archéo Lex, elles peuvent être utilisées vu la proximité des deux projets.

https://archeo-lex.fr/listes

