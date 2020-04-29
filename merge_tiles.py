import os
from osgeo import gdal
from log4p import Log
import time
import click
import math

log = Log(__file__)


@click.command()
@click.option(
    '--input-folder', '-f',
    help='Tiles folder. For example, D:/tilemap/8',
    required=False)
@click.option(
    '--scope', '-s', multiple=True,
    help='The geographical range of map, [min_row, max_row, min_col, max_col]. For example, [80574.81260346594, 176816.21260346594, 1012.1329692534127, 56528.33296925342]',
    default=[80574.81260346594, 176816.21260346594, 1012.1329692534127, 56528.33296925342],
    required=False)
@click.option(
    '--origin', multiple=True,
    help='The origin x and y of tiles. For example, [-5123300, 10002300]',
    default=[-5123300, 10002300],
    required=False)
@click.option(
    '--resolution',
    help='The tile resolution. For example, 13.229193125052918',
    type=float,
    required=True)
@click.option(
    '--tilesize', '-t',
    help='The tile size, the default is 256.',
    type=int,
    default=256,
    required=False)
@click.option(
    '--merged-file', '-o',
    help='The name of merged file. For example, res/2019_image_data.',
    required=True)
def main(input_folder, scope, origin, resolution, tilesize, merged_file):
    originX = origin[0]
    originY = origin[1]
    minX = scope[0]
    maxX = scope[1]
    minY = scope[2]
    maxY = scope[3]

    min_col, min_row = get_col_row(originX, originY, minX, maxY, tilesize, resolution)
    max_col, max_row =  get_col_row(originX, originY, maxX, minY, tilesize, resolution)

    tilewidth = max_col - min_col + 1
    tileheight = max_row - min_row + 1

    name = os.path.basename(merged_file).split('.')[0]
    suffix = os.path.splitext(merged_file)[1]
    cur_path = os.path.dirname(merged_file)
    temp_file = os.path.join(cur_path, name + "_temp" + suffix)    # 没有纠偏的临时文件

    start = time.time()

    if os.path.exists(temp_file):
        os.remove(temp_file)

    log.info('开始创建merged_file...')
    dr = gdal.GetDriverByName("GTiff")
    out_ds = dr.Create(temp_file, tilewidth * tilesize, tileheight * tilesize, 3, gdal.GDT_Int16, options=["BIGTIFF=YES", "COMPRESS=LZW", "TILED=YES", "INTERLEAVE=PIXEL"])
    log.info('创建成功.')

    out_r = out_ds.GetRasterBand(1)
    out_g = out_ds.GetRasterBand(2)
    out_b = out_ds.GetRasterBand(3)

    log.info('开始拼接')
    icount = 0
    for root, subDir, files in os.walk(input_folder):  # e:/8_res E:/Source code/TrafficDataAnalysis/Spider/res/tilemap/5
        for filename in files:
            ds = gdal.Open(os.path.join(root, filename))
            redBand = ds.GetRasterBand(1)
            greenBand = ds.GetRasterBand(2)
            blueBand = ds.GetRasterBand(3)
            r = redBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, gdal.GDT_Int16)
            g = greenBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, gdal.GDT_Int16)
            b = blueBand.ReadRaster(0, 0, tilesize, tilesize, tilesize, tilesize, gdal.GDT_Int16)

            name = os.path.splitext(filename)[0]
            y = int(name.split('_')[0])
            x = int(name.split('_')[1])
            out_r.WriteRaster((x - min_col) * tilesize, (y - min_row) * tilesize, tilesize, tilesize, r, tilesize, tilesize)
            out_g.WriteRaster((x - min_col) * tilesize, (y - min_row) * tilesize, tilesize, tilesize, g, tilesize, tilesize)
            out_b.WriteRaster((x - min_col) * tilesize, (y - min_row) * tilesize, tilesize, tilesize, b, tilesize, tilesize)

            # out_r.FlushCache()
            # out_g.FlushCache()
            # out_b.FlushCache()

            icount += 1
            # print(icount)
            if icount % 1000 == 0:
                print(icount)
    log.info('拼接完成.')
    out_ds = None
    dr = None

    log.info("开始影像纠偏...")
    gcp_x0 = math.floor(((minX - originX) - min_col * (resolution * tilesize)) / resolution)
    gcp_y0 = math.floor(((originY - maxY) - min_row * (resolution * tilesize)) / resolution)
    gcp_x1 = tilewidth * tilesize - (tilesize - math.floor(((maxX - originX) - max_col * (resolution * tilesize)) / resolution))
    gcp_y1 = tileheight * tilesize - (tilesize - math.floor(((originY - minY) - max_row * (resolution * tilesize)) / resolution))

    gcp_list = [gdal.GCP(minX, maxY, 0, gcp_x0, gcp_y0),
                gdal.GCP(maxX, maxY, 0, gcp_x1, gcp_y0),
                gdal.GCP(minX, minY, 0, gcp_x0, gcp_y1),
                gdal.GCP(maxX, minY, 0, gcp_x1, gcp_y1)]

    tmp_ds = gdal.Open(temp_file, 1)
    # gdal的config放在creationOptions参数里面
    translateOptions = gdal.TranslateOptions(format='GTiff', creationOptions=["BIGTIFF=YES", "COMPRESS=LZW"], GCPs=gcp_list)
    gdal.Translate(merged_file, tmp_ds, options=translateOptions)
    tmp_ds = None
    log.info("影像纠偏完成.")

    log.info("开始构建影像金字塔...")
    out_ds = gdal.OpenEx(merged_file, gdal.OF_RASTER | gdal.OF_READONLY)
    gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
    out_ds.BuildOverviews("nearest", range(2, 16, 2))  # 第二个参数表示建立多少级金字塔, QGIS里面默认是2,4,8,16
    out_ds=None
    log.info("影像金字塔构建成功.")

    end = time.time()
    print("所有操作OK!耗时" + str(end - start))


def get_col_row(x0, y0, x, y, size, resolution):
    col = math.floor(math.fabs((x0 - x) / (size * resolution)))
    row = math.floor(math.fabs((y0 - y) / (size * resolution)))

    return col, row


if __name__ == '__main__':
    main()
