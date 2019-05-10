<img src="/logo_apidae.svg" alt="logo apidae" width="240px"/>

# Apidae Date Consumer

## Description
Module permettant le traitement des messages en provenance du bus de messagerie à destination du module Apidae Date.

## Utilisation
Les types de messages supportés par le client sont les suivants :

### Mise à jour d'une saisie sur une période d'ouverture (operation UPDATE_PERIOD)
Exemple de message à transmettre dans le bus de messagerie :
```json
{
  "operation":"UPDATE_PERIOD",
  "periodId":"123456",
  "updatedObject":{
    "externalType":"PATRIMOINE_CULTUREL",
    "externalRef":"456789",
    "startDate":"2018-01-01",
    "endDate":"2018-12-31",
    "timePeriods":[
      {
        "type":"opening",
        "weekdays":["SAT","SUN"],
        "timeFrames":[{"startTime":"12:35","endTime":null,"recurrence":null}]
      }
    ]
  }
}
```
L'objet transmis dans le champ "updatedObject" sera fusionné avec la saisie existante associée à la période d'ouverture "123456".

### Duplication d'une saisie sur une période d'ouverture (operation DUPLICATE_PERIOD)
Exemple de message à transmettre dans le bus de messagerie :
```json
{
  "operation":"DUPLICATE_PERIOD",
  "sourceObjectId":"123456",
  "duplicatedObjectId":"456789",
  "userId":1234
}
```
où :
  - `sourceObjectId` est l'identifiant de période dont la saisie doit être dupliquée
  - `duplicatedObjectId` est l'identifiant de période à associer à la saisie dupliquée
  - `userId` est l'identifiant de l'utilisateur Apidae à l'origine de la duplication

### Suppression d'une saisie sur une période d'ouverture (operation DELETE_PERIOD)
Exemple de message à transmettre dans le bus de messagerie :
```json
{
  "operation":"DELETE_PERIOD",
  "periodId":"123456"
}
```
où `periodId` est l'identifiant de période dont la saisie doit être supprimée.


## Développement
### Environnement technique
  - Node.js & npm
  - Apache CouchDB

### Mise en place de l'environnement
  - Cloner le repo
  - `npm install`
  - `npm start` pour démarrer le script en local, avec rechargement automatique du code
