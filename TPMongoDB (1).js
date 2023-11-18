/**Manel Cheraiet___Ibtissem Laribi**/



//La base de données choisie est une base de la population mondiale « World Population Dataset »
 

//Source : https://www.kaggle.com/datasets/iamsouravbanerjee/world-population-dataset


//1. Afficher les cinq premiers pays dont la population la plus élevées en 2023
db.Populations_Mondial.find( { rank: { $in:[1,2,3,4,5]}},{country:1,rank:1,_id:0}); 

 //2.Afficher le pourcentage de population des cinq 1er pays par rapport à la population mondiale en 2023(utilisation de $gt).
db.Populations_Mondial.find({worldPercentage:{$gt:0.03}},{country:1,worldPercentage:1,_id:0}); 

 
//3.  Récupérer tous les données ou le pays est soit L’inde ou la Chine(Utilisation de $IN).
db. Populations_Mondial.find({ "country": { $in: ["India", "China"] } })
 
 
 

//4.  Récupérer tous les pays avec une superficie totale supérieure a 3M de KM et une densité de population inférieur a 500 personnes/KM.
db.Populations_Mondial.find({ "area": { $gt: 3000000 }, "density": { $lt: 500 } },{country:1,_id:0})

 

//5. Afficher tous les pays ou leurs nom commence par une letttre ente “A” et ‘’F’’  (Utilisation des expressions régulières regex)
db.Populations_Mondial.find({ "country": { $regex: /^[A-F]/ } },{country:1,_id:0})

 
 
 

//6.  Afficher tous les pays ou leurs nom ne contient pas la lettre ‘’a’’
db. Populations_Mondial.find({ "country": { $not: /a/ } })
 
 
 
 
 

//7.  Récupérer tous les documents où le pays contient exactement cinq caractères :
db.Populations_Mondial.find({ "country": { $regex: /^.{5}$/ } },{country:1,_id:0})
 
 
 

//8.  Récupérer tous les noms des pays qui se termine par "ia" (sans tenir compte de la casse) :
db.Populations_Mondial.find({ "country": { $regex: /ia$/, $options: "i" } },{country:1,_id:0})
 
 

//9. Pour afficher le classement des trois premiers pays dont le taux de croissance (`growthRate`) est supérieur à 0.0039, on utilise les opérations `match` et `sort` dans la requête d'agrégation. 


// La première étape `$match` filtre les documents où le `growthRate` est supérieur à 0.0039.
// Ensuite, l'étape `$sort` trie les documents en fonction du `growthRate` dans l'ordre décroissant (-1 indique l'ordre décroissant).
// L'étape `$limit` permet de limiter les résultats aux trois premiers pays.
// Enfin, l'étape `$project` spécifie les champs à inclure dans les résultats (dans cet exemple, nous incluons le nom du pays (`country`), le taux de croissance (`growthRate`) et le classement (`rank`)).
//Cela donnera le classement des trois premiers pays dont le taux de croissance est supérieur à 0.0039, triés par ordre décroissant du taux de croissance.
 

db.population.aggregate([
  { $match: { growthRate: { $gt: 0.0039 } } },
  { $sort: { growthRate: -1 } },
  { $limit: 3 },
  { $project: { _id: 0, country: 1, growthRate: 1, rank: 1 } }
])

//10.cette requête permet de calculer  la somme du pourcentage mondial (`worldPercentage`) pour trois pays de notre choix (États-Unis, Chine et India) 

db.Populations_Mondial.aggregate([
  { $match: { country: { $in: ["United States", "China", "India"] } } },
  { $group: { _id: null, totalWorldPercentage: { $sum: "$worldPercentage" } } },
  { $project: { _id: 0, totalWorldPercentage: 1 } }
])

//11 Cette requête trie les documents par ordre décroissant de superficie (area), limite le résultat à un seul document en utilisant $limit, puis utilise $project pour renvoyer uniquement le champ country dans le résultat.

//Cela vous donnera les trois  pays ayant la plus grande superficie.

  db.Population_Mondial.aggregate([  {    $sort: { area: -1 }},{  $limit: 3},{  $project: {  _id: 0, country: 1}}])

//12. La requête calcule la différence entre les champs pop2022 et pop2023 pour chaque document de la collection Populations_Mondial,
// puis renvoie les trois premiers documents triés par ordre décroissant de la différence
  db.Populations_Mondial.aggregate([
    {
      $project: {
        _id: 0,
        country: 1,
        populationDifference: { $subtract: ["$pop2023", "$pop2022"] }
      }
    }, { $limit :3},{$sort:{populationDifference:-1}}
  ])

  

//13 cette requête donnera les pays où le taux de chômage est supérieur à 10, avec les champs "country" et "unemployment_rate"et pop2023 affichés.

db.Populations_Mondialopulation.aggregate([
    {
      $lookup: {
        from: "worldPopulation",
        localField: "country",
        foreignField: "country",
        as: "worldPopulation"
      }
    },
    {
      $match: {
        "worldPopulation.unemployment_rate": { $gt: 10 }
      }
    },
    {
      $project: {
        _id: 0,
        country: 1,
        unemployment_rate: "$worldPopulation.unemployment_rate"
      }
    }
  ])
  
// 14. Cette requête effectue une jointure externe (gauche) entre les collections "Populations_Mondial" et "worldPopulation" en utilisant les champs "country" comme clés de jointure. 
//Elle utilise également une pipeline avec l'opération $lookup pour spécifier la condition de jointure, puis utilise l'opération $unwind pour dérouler le tableau "population".

  db.Populations_Mondial.aggregate([
    {
      $lookup: {
        from: "worldPopulation",
        let: { countryName: "$country" },
        pipeline: [
          {
            $match: {
              $expr: { $eq: ["$$countryName", "$country"] }
            }
          }
        ],
        as: "population"
      }
    },
    {
      $unwind: {
        path: "$population",
        preserveNullAndEmptyArrays: true
      }
    }
  ]);
  

 

//15 La requête effectue une agrégation sur la collection "worldPopulation" en utilisant l'opération $unwind pour dérouler le tableau "language" et calcule ensuite la moyenne du taux de chômage pour chaque langue. Les résultats sont ensuite triés par ordre décroissant de la moyenne du taux de chômage et les trois premiers résultats sont limités.
db.worldPopulation.aggregate( [
  
    {
      $unwind: { path: "$language", preserveNullAndEmptyArrays: true }
    },  {
      $group:  {
          _id: "$language",
          average_unemployment_rate: { $avg: "$unemployment_rate" }}
    },{$limit:3},
       {
      $sort: { "average_unemployment_rate": -1 }
    }
 ] )

 //16 
//La requête utilise l'opération $lookup pour effectuer une jointure entre les collections "Populations_Mondiales" et "worldPopulation" sur le champ "country". 
//Ensuite, l'opération $match est utilisée pour filtrer les documents en ne sélectionnant que ceux dont le champ "money" dans la collection "worldPopulation" est égal à "euro". 
//Enfin, l'opération $project est utilisée pour projeter uniquement les champs "country" et "netChange" du résultat.
 db.Populations_Mondial.aggregate([
    {
      $lookup: {
        from: "worldPopulation",
        localField: "country",
        foreignField: "country",
        as: "worldPopulation"
      }
    },
    {
      $match: {
        "worldPopulation.money": "Euro" }
      
    },
    {
      $project: {
        _id: 0,
        country: 1,
       netChange:1
      }
    }
  ])
//17.pour calculer la moyenne du taux de chômage pour les pays dont la langue est "anglais" à partir de la collection donnée (world Population) 
  db.worldPopulation.aggregate([
    {
      $match: {
        language: "English"
      }
    },
    {
      $group: {
        _id: null,
        average_unemployment_rate: { $avg: "$unemployment_rate" }
      }
    }
  ])
//18.   Afficher les pays qui satisfont l'une des conditions suivantes :

//Le nom de la capitale commence par la lettre "A" (insensible à la casse).
//La langue contient le mot "English" (insensible à la casse).
//La monnaie contient le mot "Euro" (insensible à la casse).

db.worldPopulation.aggregate([
  {
    $match: {
      $or: [
        { "capital": { $regex: /^A/i } },
        { "language": { $regex: /English/i } },
        { "money": { $regex: /Euro/i } }
      ]
    }
  },
  {
    $project: {
      _id: 0,
      country: 1,
      capital: 1,
      language: 1,
      money: 1
    }
  }
])


