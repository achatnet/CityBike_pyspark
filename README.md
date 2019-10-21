OBJECT: TEST

Data: Static geographical information of CityBike‘s stations in Brisbane (“Brisbane_CityBike.json”)



Instructions: 
Propose a code to perform a clustering based on either the location or characteristics of bike stations. Any clustering library can be used no assessment is done on the choice of the library. The data is a subset provided by the business.
It should be developed as close as a real industrialized process. In production this code should be launched daily on 10 Go of data, the choice of the platform is up to you it can be run on a Spark cluster or Kubernetes Cluster or Azure function.
Languages: Either Scala or Python3 (or C# if on Azure function).
Deliveries: The delivery of this test should contain:
•	Code: A zip file containing all your code or a link to a repository
•	Readme: How the job is launched, and short description of your program
•	Output: directory with the clustering result
•	Backlog: The User Stories & Tasks (done or to do) related to industrializing this code.



Solution:
La solution proposé contient 4 fonctions dont le rôle et les paramètres de chacune peut être consulter plus bas 


Pour industrialiser de job, il va falloir avant tout initialiser les paramètres dans le fichier cityBike.py, à la base on devait avoir un fichier de config
pour les paramètres, mais pour des contraintes de temps, j'ai opté à cetet méthode.

Les paramètres à initialiser sont les suivant:
 
n_cluster: int
    user-defined number of clusters

account_name : string
    Storage account name

account_key: string 
    Storage account key
	
container_name: string
    Storage container name (bucket)
	
source_file_path: string
	Source file with the full path storage



Schuduling:
Le job peut être déployer dans le service HDInsight, à noter que le type des clusters à configurer doit être spark dans notre cas.

On peut aussi  schuduler le job directement sur azure Databricks (ce que j'ai fait pour les tests), 
dans notre cas, une planification pour une exécution quotidienne répondera à notre besoin


Output:

Une fois l'exécution terminé, un fichier contenant les informations suivantes sera généré dans le dossier output qui se trouve au meême niveau 
que le dossier input, le nom de fichier sera le nom de fichier input sans l'extension + ".csv"
Le contenu de fichier généré sera le suivant:
id, latitude, longitude,prediction


Exemple output:
+------+----------+----------+--------------------+----------+
|    id|  latitude| longitude|            features|prediction|
+------+----------+----------+--------------------+----------+
| 122.0|-27.482279|153.028723|[-27.482279,153.0...|         0|
|  91.0| -27.47059|153.036046|[-27.47059,153.03...|         1|
|  75.0|-27.461881|153.046986|[-27.461881,153.0...|         1|
|  99.0|-27.469658|153.016696|[-27.469658,153.0...|         0|
| 109.0| -27.48172| 153.00436|[-27.48172,153.00...|         0|
| 149.0|-27.493626|153.001482|[-27.493626,153.0...|         0|
| 139.0|-27.476076|153.002459|[-27.476076,153.0...|         0|
|  24.0|-27.493963|153.011938|[-27.493963,153.0...|         0|
| 117.0|-27.482197|153.020894|[-27.482197,153.0...|         0|
|  73.0|-27.465226|153.050864|[-27.465226,153.0...|         1|
|1101.0|-27.468447|153.024662|[-27.468447,153.0...|         1|
|  23.0|-27.473021|153.025988|[-27.473021,153.0...|         1|
|  54.0|-27.457825|153.036866|[-27.457825,153.0...|         1|
|  93.0| -27.48148| 153.02368|[-27.48148,153.02...|         0|
|  31.0|-27.467464|153.022094|[-27.467464,153.0...|         1|
|  97.0|-27.499963|153.017633|[-27.499963,153.0...|         0|
| 147.0|-27.490776|152.994747|[-27.490776,152.9...|         0|
|  77.0|-27.458199|153.041688|[-27.458199,153.0...|         1|







Fuctions:

def read_source_file(account_name, account_key, container_name, source_file_path):
  """
  Read source file from azure container

  Parameters
  ----------
  account_name : string
      Storage account name

  account_key: string 
      Storage account key

  container_name: string
      Storage container name (bucket)

  source_file_path: string
      Source file with the full path storage

  Returns
  -------
  DataFrame 
  """
 


def clean_data(data):
  """
  Clean data and return a dataframe cleaned

  Parameters
  ----------
  data : dataframe

  Returns
  -------
  DataFrame 
  """
  
 def vec_ass(data):
  """
  Assemble latitude and longitude columns in features column

  Parameters
  ----------
  data : dataframe

  Returns
  -------
  DataFrame 
  """
 

def kmeans(df, nb_cluster=2, seed=1):
  """
  Applying KMeans Alg with nb_cluster and seed parameters defined by the user

  Parameters
  ----------
  df : DataFrame

  nb_cluster : int
      number of clusters, default value is 2

  seed: int 
      seed parameter, default value is 1

  Returns
  -------
  DataFrame with clusters info column
  """
  


