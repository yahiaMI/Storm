#This bolt analyzes trigger SentimentAnalysisBolt.py wich analyzes the
# tweet text sentiment using VADER sentiment analysis tools. VADER is a
# tool from the NLTK tool.


from pystorm.bolt import Bolt
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class SentimentAnalysisBolt(Bolt):
	def process(self, tup):

		# extract the sentence
		sentence = tup.values[0]  

		sid = SentimentIntensityAnalyzer()
		ss = sid.polarity_scores(sentence)
		tuple_result = (str(ss['neg']),str(ss['pos']),str(ss['neu']))
		self.emit(tuple_result)
		

SentimentAnalysisBolt().run()




