# coding: utf-8
import os
import urllib2
import PIL.Image as Image
import math

pi = 3.1415926535

# print ((math.pow(2,-11)*(3346+1)-1)*180);
# print ((360*math.atan(pow(math.e,(1-pow(2,-11)*(1787+1))*math.pi))/math.pi-90));

print(math.pow(2,16) * ((114.130934 + 180) / 360))
print(math.pow(2,16) * ((114.026838 + 180) / 360))

print(math.pow(2,16) * (1 - (math.log(math.tan(pi*22.751616/180) + 1/math.cos(pi*22.751616/180)) / pi)) / 2)
print(math.pow(2,16) * (1 - (math.log(math.tan(pi*22.85812/180) + 1/math.cos(pi*22.85812/180)) / pi)) / 2)

print(math.pow(2, 12) * ((113.815535 + 180) / 360))
print(math.pow(2, 12) * ((114.466221 + 180) / 360))

print(math.pow(2, 12) * (1 - (math.log(math.tan(pi * 22.113426 / 180) + 1 / math.cos(pi * 22.113426 / 180)) / pi)) / 2)
print(math.pow(2, 12) * (1 - (math.log(math.tan(pi * 22.5951022 / 180) + 1 / math.cos(pi * 22.5951022 / 180)) / pi)) / 2)


def geturl_ditu(y, x):
    url_ditu = 'http://t0.tianditu.gov.cn/img_w/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&' \
               'LAYER=img&STYLE=default&TILEMATRIXSET=w&FORMAT=tiles&TILEMATRIX={0}&' \
               'TILEROW={1}&TILECOL={2}&tk=b04ac957fe91b9c0e78877230acefe24'.format(12,str(y),str(x))
# url_ditu = 'http://t5.tianditu.gov.cn/DataServer?T=img_w&x={0}&y={1}&l=12'.format(str(x),str(y))
    return url_ditu


def geturl_dituzhuji(y, x):
    url_dituzhuji = 'http://t0.tianditu.gov.cn/cia_w/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&' \
               'LAYER=img&STYLE=default&TILEMATRIXSET=w&FORMAT=tiles&TILEMATRIX={0}&' \
               'TILEROW={1}&TILECOL={2}&tk=b04ac957fe91b9c0e78877230acefe24'.format(12,str(y),str(x))
    return url_dituzhuji


# Capture Parameter
tilesize = 256
tiletop = 1783  # 28487
tileheight = 6  # 25
tileleft = 3342  # 53521
tilewidth = 8  # 25

ditu_bigimg = Image.new('RGBA', (tilesize * tilewidth, tilesize * tileheight))
dituzhuji_bigimg = Image.new('RGBA', (tilesize * tilewidth, tilesize * tileheight))
for y in range(0, tileheight):
    for x in range(0, tilewidth):
        # Download Ditu
        ditu_imgurl = geturl_ditu(tiletop + y, tileleft + x)
        ditu_img = urllib2.urlopen(ditu_imgurl)
        ditu_file = open('image-data/ditu-' + str(y) + '_' + str(x) + '.png', "wb")
        ditu_file.write(ditu_img.read())
        ditu_file.close()
        print('ditu-' + str(y) + '_' + str(x) + '.png' + '  Saved!')
        ditu_bigimg.paste(Image.open('image-data/ditu-' + str(y) + '_' + str(x) + '.png'), (x * tilesize, y * tilesize))
        # Download DituZhuji
        dituzhuji_imgurl = geturl_dituzhuji(tiletop + y, tileleft + x)
        dituzhuji_img = urllib2.urlopen(dituzhuji_imgurl)
        dituzhuji_file = open('image-anno/dituzhuji-' + str(y) + '_' + str(x) + '.png', "wb")
        dituzhuji_file.write(dituzhuji_img.read())
        dituzhuji_file.close()
        print('dituzhuji-' + str(y) + '_' + str(x) + '.png' + '  Saved!')
        dituzhuji_bigimg.paste(Image.open('image-anno/dituzhuji-' + str(y) + '_' + str(x) + '.png'),
                               (x * tilesize, y * tilesize))

ditu_bigimg.save('ditu.png')
print('ditu.png  Saved!')
dituzhuji_bigimg.save('dituzhuji.png')
print('dituzhuji.png  Saved!')
dituall = Image.new('RGBA', (tilesize * tilewidth, tilesize * tileheight))
dituall.paste(Image.open('ditu.png'), (0, 0))
dituall.paste(Image.open('dituzhuji.png'), (0, 0), Image.open('dituzhuji.png'))
dituall.save('dituall.png')
print('dituall.png  Saved!')
