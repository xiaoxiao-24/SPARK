# spark 2.2 & python 3.6.1
CompoPath="file:///Users/xiaoxiao/Documents/BigData/data_sante/compo.csv"

import pandas as pd
#med_substence=pd.read_csv(r'file:///Users/xiaoxiao/Documents/BigData/data_sante/compo.csv', sep = '\t') 

column_names = ['1','2','3','4','5','6','7','8']
med_substence = pd.read_csv(CompoPath, sep = '\t', header = None, names = column_names)
print(med_substence[['1']])
print(med_substence.iloc[:,0])

# list to string
# methode 1
nuage_str = ",".join(med_substence.iloc[:,0])
# methode 2
nuage_str2 = str(med_substence.iloc[:,0]).strip('[]')

from wordcloud import WordCloud,ImageColorGenerator  
# get font from mac system
fonts_path = "/System/Library/Fonts/Avenir.ttc" 

wc = WordCloud(font_path = fonts_path, width=800, height=400, background_color='white').generate(nuage_str)

image = wc.to_image()
image.show()