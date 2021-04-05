from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType

from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# read files from HFS
input_base_path = '/yelp_dataset/yelp_academic_dataset_'
review_path = input_base_path + 'review.json'
business_path = input_base_path + 'business.json'

output_path = '/output/'
weighted_reviews_path = output_path + 'weighted_reviews.csv'
model_path = output_path + 'model_lv.parquet'

spark = SparkSession.builder.appName("Yelp Review Classifier").getOrCreate()

# 209393 rows
df_biz = spark.read.json(business_path).cache() # multiLine=True)
# 8,021,122 rows
df_review = spark.read.json(review_path)# multiLine=True)


def weighted_biz_rating(rating, quantity):
    #source: https://math.stackexchange.com/questions/942738/algorithm-to-calculate-rating-based-on-multiple-reviews-using-both-review-score
    rating_weight = 0.8
    #what I think is a moderate amount of reviews
    quantity_weight = 200
    return (rating * rating_weight) + 5 * (1 - rating_weight) * (1- 2.71828 ** (-quantity / quantity_weight))


standardizeUDF = F.udf(weighted_biz_rating, FloatType())
df_biz = df_biz.withColumnRenamed('stars', 'stars_business')
df_biz = df_biz.withColumn('stars_business_weighted', standardizeUDF(df_biz['stars_business'], df_biz['review_count']))
df_biz.printSchema()
df_biz.limit(5).show()

df_review = df_review.withColumnRenamed('stars', 'stars_review')
df_review.printSchema()
df_review.limit(5).show()

df = df_review.join(df_biz, ['business_id']).cache()
df.groupby('state').agg(F.count('review_id').alias('review_count')).sort(F.desc('review_count')).show()
df.groupby('state', 'city').agg(F.count('review_id').alias('review_count')).sort(F.desc('review_count')).show()

df_biz.groupby('state').agg(F.count('business_id').alias('business_count')).sort(F.desc('business_count')).show()
df_biz.groupby('state', 'city').agg(F.count('business_id').alias('business_count')).sort(F.desc('business_count')).show()
df_biz.select('categories').dropDuplicates().count()

df_lv = df.where((df.state == 'NV') & (df.city == 'Las Vegas')).cache()
df_ph = df.where((df.state == 'AZ') & (df.city == 'Phoenix'))
df_to = df.where((df.state == 'ON') & (df.city == 'Toronto'))


#df_biz.where( (df_biz['categories'] .contains( 'Chinese')) & (df_biz.city != 'Las Vegas') ).sort(F.desc('stars_business_weighted'))[['name', 'review_count', 'stars_business_weighted', 'city', 'categories']].show().head(20)
df_weighted = df_biz.where((df_biz.city != 'Las Vegas')).sort(F.desc('stars_business_weighted'))[['name', 'review_count', 'stars_business_weighted', 'city', 'categories']]
df_weighted.show(20)
df_weighted.write.mode("overwrite").csv(weighted_reviews_path)

#based off of: https://github.com/kapsali29/sentiment-Spark/blob/master/Pyspark-sent_analysis.ipynb

def transform(star):
    if star >= 3.0:
        return 1.0
    else:
        return 0.0


transformer = F.udf(transform)

# add word features
regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "into", "through", "before", "after", "to", "on", "off", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "other", "such", "no", "nor", "own", "so", "than", "s", "t", "can", "will", "just", "don", "now", "amp"]

stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(stopwords)
# bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)

pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors])


def predict(df_city, is_train, is_write=False):
    df_labels = df_city.withColumn('label', transformer(df_city['stars_review'])).select('text', 'stars_review', F.col('label').cast(DoubleType())).cache()

    # add the additional word processing fields, i.e tokenize etc
    pipelineFit = pipeline.fit(df_labels)
    dataset = pipelineFit.transform(df_labels)
    dataset.limit(5).show()

    lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)

    # fit the data-sets and make prediction
    if is_train:
        (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
        #print("Training Dataset Count: " + str(trainingData.count()))
        #print("Test Dataset Count: " + str(testData.count()))
        lrModel = lr.fit(trainingData)
        predictions = lrModel.transform(testData)

    else:
        lrModel = lr.fit(dataset)
        predictions = lrModel.transform(dataset)

    if is_write:
        predictions.write.mode("overwrite").parquet(model_path)
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    return evaluator.evaluate(predictions)


prediction_lv = predict(df_lv, is_train=True, is_write=True)
print(prediction_lv)

prediction_ph = predict(df_ph, is_train=False)
print(prediction_ph)

prediction_to = predict(df_to, is_train=False)
print(prediction_to)
