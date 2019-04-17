# spark 2.3.3 & python 3.6.6
import pandas as pd

CompoPath="C:\\Users\\Xiaoxiao\\Downloads\\CIS_COMPO_bdpm.csv"
BIPath="C:\\Users\\Xiaoxiao\\Downloads\\BenchBI.txt"

column_names = ['1','2','3','4','5','6','7','8']
med_substence = pd.read_csv(CompoPath, sep = '\t', header = None, names = column_names)
print(med_substence[['1']])
print(med_substence.iloc[:,0])

BIFile = sc.textFile(BIPath)
#define and call a function
#-------------------------------------------------
def WordList(s):
	words = s.split(" ")
	words = words.split(",")
	return words
words = BIFile.map(WordList)

# list to string
# methode 1
nuage_str = ",".join(med_substence.iloc[:,0])
# methode 2
nuage_str2 = str(med_substence.iloc[:,0]).strip('[]')

from wordcloud import WordCloud,ImageColorGenerator  
import matplotlib
# get font from mac system
fonts_path = "/System/Library/Fonts/Avenir.ttc" 
# get font from windows system
fonts_path = "C:\\Windows\\Fonts\\ARLRDBD.TTF"

wc = WordCloud(font_path = fonts_path, width=800, height=400, background_color='white').generate(nuage_str)

image = wc.to_image()
image.show()