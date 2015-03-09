# -*- coding: utf-8 -*-
# 
# Loifrançaise – Bibliothèque de manipulation de la loi française
# – ce module télécharge les bases en Open Data et les installe
# – ce module télécharge diverses donnés et métadonnées depuis Légifrance
# 
# This program is free software. It comes without any warranty, to
# the extent permitted by applicable law. You can redistribute it
# and/or modify it under the terms of the Do What The Fuck You Want
# To Public License, Version 2, as published by Sam Hocevar. See
# the LICENSE file for more details.

# Imports
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import string
import shutil

import ftplib
import subprocess
from datetime import datetime
from path import path
from bs4 import BeautifulSoup
import peewee

from marcheolex import NonImplementeException, \
                       NomBaseException, FondationNonUniqueException, \
                       FondationNonTrouveeException
from marcheolex import bases, serveurs, fichiers_base, fichiers_majo
from marcheolex.basededonnees import Livraison  
from marcheolex.utilitaires import verif_taille

#
# Constantes
#

# Bases gérées
bases = ['LEGI'] #['JORF', 'LEGI', 'KALI', 'CNIL', 'CONSTIT']

# Adresses des serveurs et noms des fichiers
# Voir http://rip.journal-officiel.gouv.fr/index.php/pages/juridiques
serveurs = {
    'JORF': ('ftp', 'ftp2.journal-officiel.gouv.fr', 'jorf', 'open1234', '/'),
    'LEGI': ('ftp', 'ftp2.journal-officiel.gouv.fr', 'legi', 'open1234', '/'),
    'KALI': ('ftp', 'ftp2.journal-officiel.gouv.fr', 'kali', 'open1234', '/'),
    'CNIL': ('ftp', 'ftp2.journal-officiel.gouv.fr', 'cnil', 'open1234', '/'),
    'CONSTIT': ('ftp', 'ftp2.journal-officiel.gouv.fr', \
                'constit', 'open1234', '/'),
    'CIRCULAIRES': ('ftp', 'echanges.dila.gouv.fr:6370', \
                    'anonymous', '', '/CIRCULAIRES/FLUX/'),
}
fichiers_base = {
    'JORF': 'Freemium_jorf_global_%Y%m%d-%H%M%S.tar.gz',
    'LEGI': 'Freemium_legi_global_%Y%m%d-%H%M%S.tar.gz',
    'KALI': 'Freemium_kali__%Y%m%d-%H%M%S.tar.gz',
    'CNIL': 'Freemium_cnil_global_%Y%m%d-%H%M%S.tar.gz',
    'CONSTIT': 'Freemium_constit_global_%Y%m%d-%H%M%S.tar.gz',
    'CIRCULAIRES': None
}
fichiers_majo = {
    'JORF': 'jorf_%Y%m%d-%H%M%S.tar.gz',
    'LEGI': 'legi_%Y%m%d-%H%M%S.tar.gz',
    'KALI': 'kali_%Y%m%d-%H%M%S.tar.gz',
    'CNIL': 'cnil_%Y%m%d-%H%M%S.tar.gz',
    'CONSTIT': 'constit_%Y%m%d-%H%M%S.tar.gz',
    'CIRCULAIRES': 'circulaire_%d%m%Y%Hh%M.tar.gz'
}


#
# Exception
#

class ImageBaseNonUniqueException(Exception):
    pass

class ImageBaseNonTrouveeException(Exception):
    pass


#
# Fonctions
#

# Sommaire:
# - obtenir_base(base, dossier, livraison): 
#     Télécharger et extraire une base de données XML
# 
# - telecharger_base(base, dossier, livraison, nom_base, nom_majo):
#     Télécharger le fichier d’une base de données XML
# 
# - telecharger_dates_base(base, livraison): 
#     Télécharger les dates de livraison d’une base de données XML
# 

# Obtenir les dates de livraison disponibles sur le serveur
# 
# @param str base 'JORF', 'LEGI', 'KALI', 'CNIL', 'CONSTIT', 'CIRCULAIRES'
# @param str|datetime livraison 'tout' pour télécharger toute la base
#                               'base' pour télécharger l’image de base
#                               objet datetime pour tout jusqu’à la date
#                               'AAAAMMJJ-HHMMSS' idem datetime
def telecharger_dates_base(base, livraison='tout'):
    
    # Vérification des paramètres
    base = base.upper()
    if base not in bases:
        raise NomBaseException()
    if livraison not in ['base','tout'] and \
       not isinstance(livraison, datetime):
         livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    if serveurs[base][0] != 'ftp':
        raise NonImplementeException()
    
    # Connexion FTP
    serveur = serveurs[base][0] + ':' + \
              '//' + serveurs[base][2] + ':' + serveurs[base][3] + \
              '@' + serveurs[base][1] + serveurs[base][4]
    
    connexion_ftp = ftplib.FTP(serveurs[base][1], \
                               serveurs[base][2], \
                               serveurs[base][3])
    
    # Reconnaître les dates des fichiers
    connexion_ftp.cwd(serveurs[base][4])
    fichiers = connexion_ftp.nlst()
    date_base = None
    dates_majo = []
    
    for fichier in fichiers:
        
        # Si c’est un fichier de dump complet
        try:
            datetime.strptime(fichier, fichiers_base[base])
            if date_base: raise ImageBaseNonUniqueException()
            date_base = datetime.strptime(fichier, fichiers_base[base])
        except ValueError:
            pass
        
        # Si c’est un fichier de dump incrémental
        try:
            dates_majo.append(datetime.strptime(fichier, fichiers_majo[base]))
        except ValueError:
            pass
    
    # Normaliser les paramètres relatifs aux dates
    dates_majo.sort()
    if not date_base:
        raise ImageBaseNonTrouveeException()
    if livraison == 'base':
        livraison = date_base
    if livraison == 'tout':
        livraison = dates_majo[-1]
    dates_majo = [date for date in dates_majo if date <= livraison]
    
    # Téléchargement du dump complet
    telecharger_cache(serveur + date_base.strftime(fichiers_base[base]),
                      os.path.join(dossier, base + 
                      date_base.strftime('-fond-%Y%m%d-%H%M%S.tar.gz')))
    
    # Téléchargement des dumps incrémentaux
    for date_majo in dates_majo:
        
        telecharger_cache(serveur + date_majo.strftime(fichiers_majo[base]),
                          os.path.join(dossier, base + 
                          date_majo.strftime('-majo-%Y%m%d-%H%M%S.tar.gz')))
    
    # Clôturer la connexion
    connexion_ftp.close()
    
    return date_base, dates_majo


# Téléchargement des bases juridiques
# 
# @param str base 'JORF', 'LEGI', 'KALI', 'CNIL', 'CONSTIT', 'CIRCULAIRES'
# @param str dossier
# @param str|datetime livraison 'tout' pour télécharger toute la base
#                               'base' pour télécharger l’image de base
#                               objet datetime pour tout jusqu’à la date
#                               'AAAAMMJJ-HHMMSS' idem datetime
# @param str nom_base format du nom de fichier de base
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @param str nom_majo format des noms de fichiers de mise à jour
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
def telecharger_base(base, dossier, livraison='tout',
                     nom_base='BASE-fond-%Y%m%d-%H%M%S.tar.gz',
                     nom_majo='BASE-majo-%Y%m%d-%H%M%S.tar.gz'):
    
    # Vérification des paramètres
    base = base.upper()
    if base not in bases:
        raise NomBaseException()
    if livraison not in ['base','tout'] and \
       not isinstance(livraison, datetime):
         livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    if serveurs[base][0] != 'ftp':
        raise NonImplementeException()
    
    # Créer le dossier des fichiers téléchargés
    path(dossier).mkdir_p()
    
    # Obtenir les dates des livraisons
    date_base, dates_majo = telecharger_dates_base(base, livraison)

    # Téléchargement du dump complet
    nom_fichier =re.sub(r'BASE', base, date_base.strftime(nom_base))
    telecharger_cache(serveur + date_base.strftime(fichiers_base[base]),
                      os.path.join(dossier, nom_fichier))
    
    # Téléchargement des dumps incrémentaux
    for date_majo in dates_majo:
        
        nom_fichier = re.sub(r'BASE', base, date_majo.strftime(nom_majo))
        telecharger_cache(serveur + date_majo.strftime(fichiers_majo[base]),
                          os.path.join(dossier, nom_fichier))
    
    return date_base, dates_majo


# Télécharger une base spécifiée à une livraison spécifiée
# 
# @param str base 'JORF', 'LEGI', 'KALI', 'CNIL', 'CONSTIT', 'CIRCULAIRES'
# @param str|datetime livraison 'tout' pour télécharger toute la base
#                               'base' pour télécharger l’image de base
#                               objet datetime pour tout jusqu’à la date
#                               'AAAAMMJJ-HHMMSS' idem datetime
# @param str cache
def obtenir_base(base, livraison='base', cache='cache'):
    
    # Vérification des paramètres
    base = base.upper()
    if base not in bases:
        raise NomBaseException()
    if livraison not in ['base','tout'] and \
       not isinstance(livraison, datetime):
        livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    
    dossier_tar = os.path.join(cache, 'tar')
    dossier_base = os.path.join(cache, 'bases-xml')

    # Télécharger les fichiers
    date_base, dates_majo = telecharger_base(base, dossier_tar, livraison)
    
    # Télécharger les fichiers
    decompresser_base(base, date_base, dates_majo, cache)
    
    # Chaînage avant des livraisons
    chainage_avant(date_base, dates_majo)


# Le tuple renvoyé correspond à (Nom, cidTexte, estUnCode, 'xml'|'xml-html'|'html'|None)
def obtenir_identifiants(cles, cache):
    
    codes, sedoc = telecharger_index_codes(cache)
    
    ncles = [''] * len(cles)
    for i in range(0, len(cles)):
        
        cle = re.sub('’', '\'', re.sub('[_-]', ' ', cles[i]))
        cle = cle[0].upper() + cle[1:].lower()
        
        if cle == 'Constitution de 1958' or cle.upper() == 'LEGITEXT000006071194':
            ncles[i] = ('Constitution de 1958', 'LEGITEXT000006071194', False, None)
        elif cle in sedoc.keys():
            ncles[i] = (re.sub('\'', '’', cle), sedoc[cle], True, None)
        elif cle.upper() in codes.keys():
            ncles[i] = (codes[cle.upper()], cle.upper(), True, None)
        else:
            ncles[i] = (None, cles[i], None, None)
    
    return ncles


def telecharger_index_codes(cache):
    
    codes = {}
    sedoc = {}
    
    # Télécharger le cas échéant le formulaire de recherche contenant le nom des codes
    path(os.path.join(cache, 'html')).mkdir_p()
    telecharger_legifrance('initRechCodeArticle.do', 'recherche.html', os.path.join(cache, 'html'), 86400)
    fichier_recherche = open(os.path.join(cache, 'html', 'recherche.html'), 'r')
    soup = BeautifulSoup(fichier_recherche.read())
    
    # Récupérer les informations
    codes_html = soup.select('select[name="cidTexte"]')[0].findAll('option')
    for code_html in codes_html:
        if code_html.has_attr('title') and code_html.has_attr('value'):
            codes[code_html.attrs['value']] = code_html.attrs['title']
            sedoc[code_html.attrs['title']] = code_html.attrs['value']
    
    return codes, sedoc


# Décompresser les fichiers téléchargés
# 
# @param str base 'JORF', 'LEGI', 'KALI', 'CNIL', 'CONSTIT', 'CIRCULAIRES'
# @param datetime date_base date du dump complet
# @param list[datetime] date_majo dates des dumps incrémentaux
# @param str cache
def decompresser_base(base, date_base, dates_majo, cache='cache'):
    
    # Vérification des paramètres
    base = base.upper()
    if base not in bases:
        raise NomBaseException()
    if not isinstance(date_base, datetime) or not isinstance(dates_majo, list):
        raise ValueError()
    for date in dates_majo:
        if not isinstance(date, datetime):
            raise ValueError()
    
    # Créer le répertoire rattaché à ce dump complet
    rep = os.path.join(cache, 'bases-xml', date_base.strftime('%Y%m%d-%H%M%S'))
    path(rep).mkdir_p()
    
    # Décompresser le dump complet
    date = date_base.strftime('%Y%m%d-%H%M%S')
    if not os.path.exists(os.path.join(rep, 'fond-' + date)) or \
       os.path.exists(os.path.join(rep, 'fond-' + date, 'erreur-tar')):
        
        if os.path.exists(os.path.join(rep, 'fond-' + date, 'erreur-tar')):
            shutil.rmtree(os.path.join(rep, 'fond-' + date))
        path(os.path.join(rep, 'fond-' + date)).mkdir_p()
        open(os.path.join(rep, 'fond-' + date, 'erreur-tar'), 'w').close()
        subprocess.call(['tar', 'xzf',
            os.path.join(cache, 'tar', base + '-fond-' + date + '.tar.gz'),
            '-C', os.path.join(rep, 'fond-' + date)])
        os.remove(os.path.join(rep, 'fond-' + date, 'erreur-tar'))
        
    
    # Inscrire cette livraison dans la base de données
    try:
        entree_livraison = Livraison.get(Livraison.date == date_base)
    except Livraison.DoesNotExist:
        entree_livraison = Livraison.create(
                date=date_base,
                type='fondation',
                base=base,
                precedente=None,
                suivante=None,
                fondation=None
            )
    entree_livraison_fondation = entree_livraison
    
    # Décompresser les dumps incrémentaux
    for date_majo in dates_majo:
        
        date = date_majo.strftime('%Y%m%d-%H%M%S')
        if not os.path.exists(os.path.join(rep, 'majo-' + date)) or \
           os.path.exists(os.path.join(rep, date)) or \
           os.path.exists(os.path.join(rep, 'majo-' + date, 'erreur-tar')):
            
            if os.path.exists(os.path.join(rep, date)):
                shutil.rmtree(os.path.join(rep, date), True)
                shutil.rmtree(os.path.join(rep, 'majo-' + date), True)
            if os.path.exists(os.path.join(rep, 'majo-' + date, 'erreur-tar')):
                shutil.rmtree(os.path.join(rep, 'majo-' + date), True)
            path(os.path.join(rep, date)).mkdir_p()
            open(os.path.join(rep, date, 'erreur-tar'), 'w').close()
            subprocess.call(['tar', 'xzf',
                os.path.join(cache, 'tar', base + '-majo-' + date + '.tar.gz'),
                '-C', rep])
            os.rename(os.path.join(rep, date),
                      os.path.join(rep, 'majo-' + date))
            os.remove(os.path.join(rep, 'majo-' + date, 'erreur-tar'))
        
        # Inscrire cette livraison dans la base de données
        try:
            entree_livraison = Livraison.get(Livraison.date == date_majo)
        except Livraison.DoesNotExist:
            entree_livraison = Livraison.create(
                    date=date_majo,
                    type='miseajour',
                    base=base,
                    precedente=entree_livraison,
                    suivante=None,
                    fondation=entree_livraison_fondation
                )



def chainage_avant(date_base, dates_majo):
    
    dates = [date_base] + dates_majo[0:-1]
    if not dates_majo:
        return
    
    entree_livraison_suivante = Livraison.get(Livraison.date == date_majo[-1])
    
    for date in dates:
        
        entree_livraison = Livraison.get(Livraison.date == date)
        
        if entree_livraison.suivante:
            break
        
        entree_livraison.suivante = entree_livraison_suivante
        entree_livraison.save()
        entree_livraison_suivante = entree_livraison



#
# Fonctions annexes
#

# Télécharger un fichier depuis Légifrance
def telecharger_legifrance(url, fichier, cache_html, force=False):
    
    return telecharger_cache('http://legifrance.gouv.fr/' + url,
                             os.path.join(cache_html, fichier), force)


# Télécharger un fichier avec cache possible
def telecharger_cache(url, fichier, force=False):
    
    if os.path.exists(fichier):
        touch = datetime.datetime.fromtimestamp(os.stat(fichier).st_mtime)
        delta = datetime.datetime.today() - touch
        
        if not force or not isinstance(force, bool) and isinstance(force, (int, long, float)) and delta.total_seconds() < force:
            print('* Téléchargement de ' + url + ' (cache)')
            return True
    
    print('* Téléchargement de ' + url)
    return telecharger(url, fichier)


# Télécharger un fichier
def telecharger(url, fichier):
    
    subprocess.call(['wget', '--output-document=' + fichier, url])
