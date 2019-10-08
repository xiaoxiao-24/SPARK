from pyspark.sql import HiveContext

hive_context = HiveContext(sc)
hive_context.sql("use oppchain_esante")
med_substance = hive_context.sql('SELECT denomination_substance FROM dm_medic_avec_composition')
print(med_substance[0])

#spark DataFrame to list
# err : sublist = list(med_substance)
sub_list = med_substance.select('denomination_substance').collect()

sub_str = []
for p in sub_list:
    sub_str += p

# row to string
nuage_str = ",".join(sub_str)

#sudo pip install matplotlib --ignore-installed pyparsing
#sudo yum install tkinter
from wordcloud import WordCloud,ImageColorGenerator 
import matplotlib
from os import path

fonts_path = "/System/Library/Fonts/Helvetica.ttc" # mac
fonts_path = "/usr/share/fonts/default/ghostscript/putri.pfa"
wc = WordCloud(font_path = fonts_path, width=800, height=400, background_color='white').generate(nuage_str)

# show image directly, mais dans sandbox il manque environnement pour display
image = wc.to_image()
image.show()