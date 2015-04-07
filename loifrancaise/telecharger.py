# -*- coding: utf-8 -*-
# 
# Loifrançaise – Bibliothèque de manipulation de la loi française
# – ce module télécharge les bases en Open Data et les installe
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
import tarfile
import ftplib
import subprocess
from datetime import datetime, timedelta, tzinfo



#
# Constantes
#

# Bases gérées
bases = ['JORF', 'JORFSIMPLE', 'LEGI', 'KALI', 'CNIL', 'CONSTIT']
# 'CIRCULAIRES': le FTP ne marche pas souvent

# Adresses des serveurs et noms des fichiers
# Voir http://rip.journal-officiel.gouv.fr/index.php/pages/juridiques
serveurs = {
    'JORF': ('ftp2.journal-officiel.gouv.fr', 21, 'jorf', 'open1234', '/'),
    'JORFSIMPLE': ('ftp2.journal-officiel.gouv.fr', 21, \
                   'jorfsimple', 'open1234', '/'),
    'LEGI': ('ftp2.journal-officiel.gouv.fr', 21, 'legi', 'open1234', '/'),
    'KALI': ('ftp2.journal-officiel.gouv.fr', 21, 'kali', 'open1234', '/'),
    'CNIL': ('ftp2.journal-officiel.gouv.fr', 21, 'cnil', 'open1234', '/'),
    'CONSTIT': ('ftp2.journal-officiel.gouv.fr', 21, \
                'constit', 'open1234', '/'),
    'CIRCULAIRES': ('ftp', 'echanges.dila.gouv.fr', 6370, \
                    'anonymous', '', '/CIRCULAIRES/FLUX/'),
}
fichiers_base = {
    'JORF': 'Freemium_jorf_global_%Y%m%d-%H%M%S.tar.gz',
    'JORFSIMPLE': 'Freemium_jorfsimple_jorf_simple_%Y%m%d-%H%M%S.tar.gz',
    'LEGI': 'Freemium_legi_global_%Y%m%d-%H%M%S.tar.gz',
    'KALI': 'Freemium_kali_global_%Y%m%d-%H%M%S.tar.gz',
    'CNIL': 'Freemium_cnil_global_%Y%m%d-%H%M%S.tar.gz',
    'CONSTIT': 'Freemium_constit_global_%Y%m%d-%H%M%S.tar.gz',
    'CIRCULAIRES': None
}
fichiers_majo = {
    'JORF': 'jorf_%Y%m%d-%H%M%S.tar.gz',
    'JORFSIMPLE': 'jorfsimple_%Y%m%d-%H%M%S.tar.gz',
    'LEGI': 'legi_%Y%m%d-%H%M%S.tar.gz',
    'KALI': 'kali_%Y%m%d-%H%M%S.tar.gz',
    'CNIL': 'cnil_%Y%m%d-%H%M%S.tar.gz',
    'CONSTIT': 'constit_%Y%m%d-%H%M%S.tar.gz',
    'CIRCULAIRES': 'circulaire_%d%m%Y%Hh%M.tar.gz'
}

# Nom du fichier contenant les fichiers à supprimer dans les mises à jour
# incrémentales
fichier_suppression_articles = 'liste_suppression_BASE.dat'

# Classe de gestion du fuseau horaire CET/CEST
class CEST(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=1) + self.dst(dt)
    def dst(self, dt):
        # DST starts last Sunday in March
        d = datetime(dt.year, 4, 1, 2)   # ends last Sunday in October
        self.dston = d - timedelta(days=d.weekday() + 1)
        d = datetime(dt.year, 11, 1, 3)
        self.dstoff = d - timedelta(days=d.weekday() + 1)
        if self.dston <=  dt.replace(tzinfo=None) < self.dstoff:
            return timedelta(hours=1)
        else:
            return timedelta(0)
    def tzname(self,dt):
        if self.dst(dt) == timedelta(0):
            return "CET"
        else:
            return "CEST"

# Noms standards de fichiers pour enregistrer des métadonnées de dates
# de mises à jour dans les bases juridiques XML
fichier_drapeau = 'installation-en-cours.txt'
fichier_livraison = 'livraison.txt'
fichier_historique = 'historique.txt'



#
# Exceptions
#

class NomBaseError(ValueError):
    pass

class StructureRepertoireException(Exception):
    pass

class ConnexionException(Exception):
    pass

class DossierIncoherentException(Exception):
    pass

class LivraisonManquanteException(Exception):
    pass



#
# Fonctions
#

# Télécharger les dates des livraisons d’une base juridique
# 
# @param str base dans ('JORF', 'JORFSIMPLE', 'LEGI', 'KALI', 'CNIL',
#                       'CONSTIT', 'CIRCULAIRES')
# @return list[datetime] dates
# @raise NomBaseError, ConnexionException, StructureRepertoireException
def telecharger_dates_base(base):
    
    # Vérification des paramètres
    if base not in bases:
        raise NomBaseError()
    
    # Connexion FTP
    try:
        connexion = ftplib.FTP()
        connexion.connect(serveurs[base][0], serveurs[base][1])
        connexion.login(serveurs[base][2], serveurs[base][3])
    except:
        connexion.close()
        raise ConnexionException()
    
    # Reconnaître les dates des fichiers
    connexion.cwd(serveurs[base][4])
    fichiers = connexion.nlst()
    
    # Clôturer la connexion
    connexion.close()
    
    # Ajouter le dump complet
    dates = []
    for fichier in fichiers:
        try:
            dates.append(datetime.strptime(fichier, fichiers_base[base]))
        except ValueError:
            pass
    
    # Vérifier l’intégrité du répertoire
    if len(dates) != 1:
        raise StructureRepertoireException()
    
    # Ajouter les dumps incrémentaux
    for fichier in fichiers:
        try:
            dates.append(datetime.strptime(fichier, fichiers_majo[base]))
        except ValueError:
            pass
    
    # Ranger les dates par ordre chronologique
    dates = sorted(dates)
    
    return dates


# Télécharger les fichiers compressés d’une base juridique
# 
# @param str base dans ('JORF', 'JORFSIMPLE', 'LEGI', 'KALI', 'CNIL', 
#                       'CONSTIT', 'CIRCULAIRES')
# @param str dossier
# @param str|int|datetime livraison -1 pour télécharger toute la base
#                                   0 pour télécharger l’image de base
#                                   objet datetime pour tout jusqu’à la date
#                                   'AAAAMMJJ-HHMMSS' idem datetime
# @param str nom_base format du nom de fichier de base
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @param str nom_majo format des noms de fichiers de mise à jour
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @return list[datetime] dates livraisons téléchargées
# @raise NomBaseError, ConnexionException, ValueError, IOError
def telecharger_base(base, dossier='.', livraison=-1,
                     nom_base='BASE-base-%Y%m%d-%H%M%S.tar.gz',
                     nom_majo='BASE-majo-%Y%m%d-%H%M%S.tar.gz'):
    
    # Vérification des paramètres
    if base not in bases:
        raise NomBaseError()
    if isinstance(livraison, (str, unicode)):
        livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    if not isinstance(livraison, (datetime, int)): raise ValueError()
    if not isinstance(dossier, (str, unicode)): raise ValueError()
    if not isinstance(livraison, (datetime, int)): raise ValueError()
    if not isinstance(nom_base, (str, unicode)): raise ValueError()
    if not isinstance(nom_majo, (str, unicode)): raise ValueError()
    
    # Créer le dossier des fichiers téléchargés
    if not os.path.exists(dossier):
        os.makedirs(dossier)
    
    # Obtenir les dates des livraisons
    dates = telecharger_dates_base(base)
    
    # Filtrer les livraisons voulues
    if isinstance(livraison, datetime):
        dates = [date for date in dates if date <= livraison]
    else:
        dates = dates[0:1+livraison%len(dates)]
    
    # Connexion FTP
    try:
        connexion = ftplib.FTP()
        connexion.connect(serveurs[base][0], serveurs[base][1])
        connexion.login(serveurs[base][2], serveurs[base][3])
    except:
        connexion.close()
        raise ConnexionException()
    
    # Téléchargement du dump complet
    nom_fichier = re.sub(r'BASE', base, dates[0].strftime(nom_base))
    telecharger_ftp_cache(connexion, serveurs[base][4], \
                          dates[0].strftime(fichiers_base[base]), \
                          os.path.join(dossier, nom_fichier))
    
    # Téléchargement des dumps incrémentaux
    for date in dates[1:]:
        
        nom_fichier = re.sub(r'BASE', base, date.strftime(nom_majo))
        telecharger_ftp_cache(connexion, serveurs[base][4], \
                              date.strftime(fichiers_majo[base]), \
                              os.path.join(dossier, nom_fichier))
    
    # Clôturer la connexion
    connexion.close()
    
    return dates


# Décompresser les fichiers de la base juridique spécifiée
# 
# @param str base dans ('JORF', 'JORFSIMPLE', 'LEGI', 'KALI', 'CNIL',
#                       'CONSTIT', 'CIRCULAIRES')
# @param str|int|datetime livraison -1 pour décompresser jusqu’à la livraison
#                                    la plus récente
#                                   0 pour décompresser la livraison de base
#                                   objet datetime pour décompresser jusqu’à la
#                                    date livraison
#                                   'AAAAMMJJ-HHMMSS' idem datetime
# @param str dossier dossier où sera installé la base juridique décompressée
# @param str cache dossier où se trouvent les fichiers téléchargés TAR gzippés
#                  des bases juridiques XML
# @param str nom_base format du nom de fichier de base
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @param str nom_majo format des noms de fichiers de mise à jour
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @return None
# @raise NomBaseError, ValueError, LivraisonManquanteException,
#        DossierIncoherentException, IOError
def decompresser_base(base, livraison=-1, dossier='.', cache='.',
                      nom_base='BASE-base-%Y%m%d-%H%M%S.tar.gz',
                      nom_majo='BASE-majo-%Y%m%d-%H%M%S.tar.gz'):
    
    # Vérification des paramètres
    if base not in bases:
        raise NomBaseError()
    if isinstance(livraison, (str, unicode)):
        livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    if not isinstance(livraison, (datetime, int)): raise ValueError()
    if not isinstance(dossier, (str, unicode)): raise ValueError()
    if not isinstance(cache, (str, unicode)): raise ValueError()
    if not isinstance(nom_base, (str, unicode)): raise ValueError()
    if not isinstance(nom_majo, (str, unicode)): raise ValueError()
    
    # Transformations de base
    nom_base = re.sub(r'BASE', base, nom_base)
    dossier_base = os.path.join(dossier, base.lower())
    
    # Calculer la liste des dumps à appliquer
    dates, toutes_dates = cache_disponible(base, cache, livraison, \
                                           nom_base, nom_majo)
    
    # Créer le répertoire rattaché à ce dump complet
    if not os.path.exists(dossier):
        os.makedirs(dossier)
    
    # Vérifier que la livraison n’est pas trop vieille et qu’un fichier indique
    # la livraison installée, sinon supprimer et refaire au propre
    supprimer = False
    livraison_installee = None
    if os.path.exists(dossier_base):
        if os.path.exists(os.path.join(dossier_base, fichier_livraison)):
            with open(os.path.join(dossier_base,fichier_livraison), 'r') as fd:
                livraison_installee = datetime.strptime(fd.read(), \
                                                        '%Y%m%d-%H%M%S')
            if livraison_installee < dates[0]:
                supprimer = True
                livraison_installee = None
        else:
            supprimer = True
    
    # Le cas échéant, supprimer seulement les fichiers concernés (pour laisser
    # par exemple des métadonnées gérées par d’autres fichiers)
    if supprimer:
        with open(os.path.join(dossier_base, fichier_drapeau), 'w') as fd:
            fd.write('Suppression en cours.\n')
        if os.path.exists(dossier_base):
            shutil.rmtree(dossier_base)
        if os.path.exists(os.path.join(dossier_base, fichier_livraison)):
            os.remove(os.path.join(dossier_base, fichier_livraison))
        if os.path.exists(os.path.join(dossier_base, fichier_historique)):
            os.remove(os.path.join(dossier_base, fichier_historique))
        os.remove(os.path.join(dossier_base, fichier_drapeau))
    
    # Vérifier qu’on peut mettre à jour le dossier
    # Cas possibles :
    # - dossier vide ou avec une version plus ancienne que le dump de base
    #    disponible : appliquer le dump de base et les dumps incrémentaux
    # - dossier avec une version plus récente que le premier dump (de base ou
    #    incrémental) disponible : appliquer les dumps incrémentaux
    # - dossier avec une version plus ancienne que le premier dump incrémental
    #    disponible et pas de dump de base disponibles : exception, on ne peut
    #    pas garantir qu’on ne va pas corrompre le dossier en omettant
    #    possiblement certains dumps incrémentaux
    if len(dates) == 0 \
     and not (livraison_installee and livraison_installee >= toutes_dates[0]):
        raise LivraisonManquanteException()
    
    # Vérifier que le dossier est dans un état cohérent
    if os.path.exists(os.path.join(dossier_base, fichier_drapeau)):
        raise DossierIncoherentException()
    
    # Décompresser l’image de base si nécessaire
    if not livraison_installee:
        
        # Pré-créer le dossier de base
        if not os.path.exists(dossier_base):
            os.makedirs(dossier_base)
        
        # Indiquer qu’un travail est en cours sur les fichiers
        with open(os.path.join(dossier_base, fichier_drapeau), 'w') as fd:
            fd.write(dates[0].strftime('Installation du dump complet ' \
                                       + '%Y%m%d-%H%M%S.\n'))
        
        # Décompresser le dump complet
        tar = tarfile.open(os.path.join(cache, dates[0].strftime(nom_base)))
        tar.extractall(dossier)
        
        # Mettre à jour les métadonnées
        with open(os.path.join(dossier_base, fichier_livraison), 'w') as fd:
            fd.write(dates[0].strftime('%Y%m%d-%H%M%S'))
        with open(os.path.join(dossier_base, fichier_historique), 'a') as fd:
            fd.write(dates[0].strftime('%Y%m%d-%H%M%S\n'))
        livraison_installee = dates[0]
        
        # Indiquer que la décompression s’est bien terminée
        os.remove(os.path.join(dossier_base, fichier_drapeau))
    
    # Décompresser les dumps incrémentaux
    for date in dates[1:]:
        if livraison_installee < date:
            decompresser_majo(base, date, dossier, cache, nom_majo)


# Décompresser une mise à jour de la base juridique spécifiée
# 
# @param str base dans ('JORF', 'JORFSIMPLE', 'LEGI', 'KALI', 'CNIL',
#                       'CONSTIT', 'CIRCULAIRES')
# @param str|datetime livraison objet datetime pour décompresser jusqu’à la
#                               date livraison
#                               'AAAAMMJJ-HHMMSS' idem datetime
# @param str dossier dossier où sera installé la base juridique décompressée
# @param str cache dossier où se trouvent les fichiers téléchargés TAR gzippés
#                  des bases juridiques XML
# @param str nom_majo format des noms de fichiers de mise à jour
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @return None
# @raise NomBaseError, ValueError, IOError
def decompresser_majo(base, livraison, dossier='.', cache='.',
                      nom_majo='BASE-majo-%Y%m%d-%H%M%S.tar.gz'):
    
    # Vérification des paramètres
    if base not in bases:
        raise NomBaseError()
    if isinstance(livraison, (str, unicode)):
        livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    if not isinstance(livraison, datetime): raise ValueError()
    if not isinstance(dossier, (str, unicode)): raise ValueError()
    if not isinstance(cache, (str, unicode)): raise ValueError()
    if not isinstance(nom_majo, (str, unicode)): raise ValueError()
    
    # Transformations de base
    nom_majo = re.sub(r'BASE', base, nom_majo)
    dossier_base = os.path.join(dossier, base.lower())
    
    # Vérifier que le dossier est dans un état cohérent
    if os.path.exists(os.path.join(dossier_base, fichier_drapeau)):
        raise DossierIncoherentException()
    
    # Indiquer qu’un travail est en cours sur les fichiers
    with open(os.path.join(dossier_base, fichier_drapeau), 'w') as fd:
        fd.write('Installation du dump incrémental '.encode('utf-8') + \
                 livraison.strftime('%Y%m%d-%H%M%S.\n').encode('utf-8'))
    
    # Décompresser le dump incrémental
    # Note : lors de la décompression, la base est dans un répertoire nommé
    #        de la date de mise à jour ; la section suivante commence par
    #        recréer artificiellement ce répertoire pour que l’extraction
    #        écrase correctement les fichiers existants, puis l’extraction a
    #        lieu, puis ce répertoire est annihilé
    fichier_suppr_arti = re.sub(r'BASE', base.lower(), \
                                          fichier_suppression_articles)
    os.mkdir(os.path.join(dossier, livraison.strftime('%Y%m%d-%H%M%S')))
    os.rename(dossier_base, \
              os.path.join(dossier, livraison.strftime('%Y%m%d-%H%M%S'), \
                           base.lower()))
    tar = tarfile.open(name=os.path.join(cache, livraison.strftime(nom_majo)))
    tar.extractall(dossier)
    os.rename(os.path.join(dossier, livraison.strftime('%Y%m%d-%H%M%S'), \
                           base.lower()), \
              dossier_base)
    if os.path.exists(os.path.join(dossier, \
     livraison.strftime('%Y%m%d-%H%M%S'), fichier_suppr_arti)):
        os.rename(os.path.join(dossier, livraison.strftime('%Y%m%d-%H%M%S'), \
                               fichier_suppr_arti), \
                  os.path.join(dossier_base, fichier_suppression_articles))
    os.rmdir(os.path.join(dossier, livraison.strftime('%Y%m%d-%H%M%S')))
    
    # Lire la liste des fichiers à supprimer
    if os.path.exists(os.path.join(dossier_base, fichier_suppr_arti)):
        with open(os.path.join(dossier_base, fichier_suppr_arti), 'r') as fd:
            suppression_fichiers = fd.readlines()
        for fichier in suppression_fichiers:
            os.remove(os.path.join(dossier, fichier.strip()))
    
    # Mettre à jour les métadonnées
    with open(os.path.join(dossier_base, fichier_livraison), 'w') as fd:
        fd.write(livraison.strftime('%Y%m%d-%H%M%S'))
    with open(os.path.join(dossier_base, fichier_historique), 'a') as fd:
        fd.write(livraison.strftime('%Y%m%d-%H%M%S\n'))
    
    # Indiquer que la décompression s’est bien terminée
    os.remove(os.path.join(dossier_base, fichier_drapeau))


# Télécharger les fichiers compressés d’une base juridique
# 
# @param str base dans ('JORF', 'JORFSIMPLE', 'LEGI', 'KALI', 'CNIL', 
#                       'CONSTIT', 'CIRCULAIRES')
# @param str|int|datetime livraison -1 pour télécharger toute la base
#                                   0 pour télécharger l’image de base
#                                   objet datetime pour tout jusqu’à la date
#                                   'AAAAMMJJ-HHMMSS' idem datetime
# @param str dossier
# @param str cache
# @param str versionnement dans ('aucun', 'git')
# @param str nom_base format du nom de fichier de base
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @param str nom_majo format des noms de fichiers de mise à jour
#                     (chaînes '%s' pour les variables de temps (cf datetime),
#                     'BASE' pour le nom de la base XML)
# @return list[datetime] dates livraisons téléchargées
# @raise NomBaseError, ConnexionException, ValueError, IOError
def obtenir_base(base, livraison, dossier='.', cache='.',
                 versionnement='aucun', telechargement='optionnel',
                 params_git={'auteur': 'Législateur',
                             'courriel': '',
                             'message': 'Livraison de la base BASE du '+
                                        '%Y-%m-%d %H:%M:%S'},
                 nom_base='BASE-base-%Y%m%d-%H%M%S.tar.gz',
                 nom_majo='BASE-majo-%Y%m%d-%H%M%S.tar.gz'):
    
    # Vérification des paramètres
    if base not in bases:
        raise NomBaseError()
    if isinstance(livraison, (str, unicode)):
        livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    if not isinstance(livraison, (datetime, int)): raise ValueError()
    if not isinstance(dossier, (str, unicode)): raise ValueError()
    if not isinstance(cache, (str, unicode)): raise ValueError()
    if not versionnement in ('aucun', 'git'): raise ValueError()
    if not telechargement in ('oui', 'non', 'optionnel'): raise ValueError()
    if not isinstance(nom_base, (str, unicode)): raise ValueError()
    if not isinstance(nom_majo, (str, unicode)): raise ValueError()
    
    # Transformations de base
    dossier_base = os.path.join(dossier, base.lower())
    cest = CEST()
    
    # Télécharger les fichiers
    if telechargement != 'non':
        if telechargement == 'oui':
            telecharger_base(base, dossier, livraison, nom_base, nom_majo)
        elif telechargement == 'optionnel':
            try:
                telecharger_base(base, dossier, livraison, nom_base, nom_majo)
            except ConnexionException:
                pass
    
    # Vérifier les fichiers téléchargés
    _, dates = cache_disponible(base, cache, livraison, nom_base, nom_majo)
    if len(dates) == 0:
        raise LivraisonManquanteException()
    
    # Décompresser les fichiers, sans versionnement = écraser le contenu
    # existant
    if versionnement == 'aucun':
        decompresser_base(base, livraison, dossier, cache, nom_base, nom_majo)
    
    # Décompresser les fichiers, avec versionnement git = chaque nouvelle
    # livraison (des fichiers XML) est enregistrée avec git
    elif versionnement == 'git':
        
        for i, date in enumerate(dates):
            
            # Effectuer la décompression
            decompresser_base(base, date, dossier, cache, nom_base, nom_majo)
            
            # Initialiser la première fois
            if i == 0:
                subprocess.call(['git', 'init', '-q'], cwd=dossier_base)
                with open(os.path.join(dossier_base, '.git', 'info', \
                          'exclude'), 'a') as fd:
                    fd.write(fichier_historique + '\n')
                    fd.write(fichier_livraison + '\n')
            
            # Inscrire les changements dans git
            subprocess.call(['git', 'add', '--all', '.'], cwd=dossier_base)
            
            # Enregistrer les changements dans git
            date_git = date.replace(tzinfo=cest).isoformat()
            auteur = params_git['auteur'].encode('utf-8')
            courriel = params_git['courriel'].encode('utf-8')
            auteur_complet = (params_git['auteur'] + ' <' + \
                              params_git['courriel'] + '>').encode('utf-8')
            message = params_git['message'].encode('utf-8')
            message = re.sub(r'BASE', base, \
                       date.strftime(params_git['message'])).encode('utf-8')
            
            subprocess.call(['git', 'commit', \
                             ('--author="' + params_git['auteur'] + ' ' + \
                               '<' + params_git['courriel'] + '>' \
                               '"').encode('utf-8'), \
                             '--date="' + date_git + '"', \
                             '-m', message, \
                             '-q', '--no-status'], \
                             cwd=dossier_base, \
                             env={ 'GIT_COMMITTER_NAME': auteur, \
                                   'GIT_COMMITTER_EMAIL': courriel, \
                                   'GIT_COMMITTER_DATE': date_git })


#
# Fonctions annexes
#

def cache_disponible(base, cache='.', livraison=-1,
                     nom_base='BASE-base-%Y%m%d-%H%M%S.tar.gz',
                     nom_majo='BASE-majo-%Y%m%d-%H%M%S.tar.gz'):
    
    # Vérification des paramètres
    if base not in bases:
        raise NomBaseError()
    if not isinstance(cache, (str, unicode)): raise ValueError()
    if isinstance(livraison, (str, unicode)):
        livraison = datetime.strptime(livraison, '%Y%m%d-%H%M%S')
    if not isinstance(livraison, (datetime, int)): raise ValueError()
    if not isinstance(nom_base, (str, unicode)): raise ValueError()
    if not isinstance(nom_majo, (str, unicode)): raise ValueError()
    
    # Transformations de base
    nom_base = re.sub(r'BASE', base, nom_base)
    nom_majo = re.sub(r'BASE', base, nom_majo)
    
    # Obtenir la liste des fichiers en cache
    dates = []
    for fichier in os.listdir(cache):
        try:
            dates.append((datetime.strptime(fichier, nom_base), 0))
        except ValueError:
            pass
        
        try:
            dates.append((datetime.strptime(fichier, nom_majo), 1))
        except ValueError:
            pass
    dates = sorted(dates)
    
    # Retirer les livraisons trop récentes
    if len(dates) == 0:
        return [], []
    if isinstance(livraison, datetime):
        dates = [date for date in dates if date[0] <= livraison]
    elif isinstance(livraison, int):
        dates = dates[0:1+livraison%len(dates)]
    
    # Extraire la dernière chaîne complète de dumps
    if sum([1 for i in range(len(dates)) if dates[i][1] == 0]) == 0:
        return [], [date[0] for date in dates]
    dates = dates[max([i for i in range(len(dates)) if dates[i][1] == 0]):]
    
    # Retirer les informations désormais inutiles
    dates = [date[0] for date in dates]
    
    return dates, dates


# Télécharger la liste des codes depuis Légifrance
from bs4 import BeautifulSoup
def telecharger_index_codes(cache):
    
    codes = {}
    sedoc = {}
    
    # Télécharger le cas échéant le formulaire de recherche contenant le nom des codes
    if not os.path.exists(os.path.join(cache, 'html')):
        os.makedirs(os.path.join(cache, 'html'))
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


# Télécharger un fichier depuis Légifrance
def telecharger_legifrance(url, fichier, cache_html, force=False):
    
    return telecharger_cache('http://legifrance.gouv.fr/' + url,
                             os.path.join(cache_html, fichier), force)


# Télécharger un fichier avec cache possible
def telecharger_cache(url, fichier, force=False):
    
    if os.path.exists(fichier):
        touch = datetime.fromtimestamp(os.stat(fichier).st_mtime)
        delta = datetime.today() - touch
        
        if not force or not isinstance(force, bool) and isinstance(force, (int, long, float)) and delta.total_seconds() < force:
            print('* Téléchargement de ' + url + ' (cache)')
            return True
    
    print('* Téléchargement de ' + url)
    return telecharger(url, fichier)


# Télécharger un fichier
def telecharger(url, fichier):
    
    subprocess.call(['wget', '--output-document=' + fichier, url])


# Télécharger un fichier sur un serveur FTP, avec cache possible
# 
# @param ftplib.FTP connexion objet FTP initialisé correctement
# @param str repertoire répertoire sur le serveur
# @param str fichier_orig nom du fichier sur le serveur
# @param str fichier_dest nom du fichier à enregistrer localement
# @param bool|int|long|float force utilisation du cache
# @return None
# @raise IOError
# 
# Si force == False : ne jamais re-télécharger un fichier déjà présent
# Si force == True : toujours télécharger le fichier, même si déjà présent
# Si force est un nombre : re-télécharger le fichier s’il est plus ancien (en
#                          secondes) que l’entier donné
def telecharger_ftp_cache(connexion, repertoire, fichier_orig, fichier_dest,
                          force=False):
    
    if os.path.exists(fichier_dest):
        touch = datetime.fromtimestamp(os.stat(fichier_dest).st_mtime)
        delta = datetime.today() - touch
        
        if not force \
         or not isinstance(force, bool) \
         and isinstance(force, (int, long, float)) \
         and delta.total_seconds() < force:
            return
    
    telecharger_ftp(connexion, repertoire, fichier_orig, fichier_dest)


# Télécharger un fichier sur un serveur FTP
# 
# @param ftplib.FTP connexion objet FTP initialisé correctement
# @param str repertoire répertoire sur le serveur
# @param str fichier_orig nom du fichier sur le serveur
# @param str fichier_dest nom du fichier à enregistrer localement
# @param bool|int|long|float force utilisation du cache
# @return None
# @raise IOError
def telecharger_ftp(connexion, repertoire, fichier_orig, fichier_dest):
    
    connexion.cwd(repertoire)
    connexion.retrbinary('RETR ' + fichier_orig, \
                         open(fichier_dest + '.part', 'wb').write)
    os.rename(fichier_dest + '.part', fichier_dest)

