# -*- coding: utf-8 -*-

from suplicmap_vector2 import get_json, crawl
import json
import click
from log4p import Log
import traceback

log = Log(__file__)

@click.command()
@click.option('--service-url', '-u',
              help='Input service url. For example, http://suplicmap.pnr.sz/dynaszmap_3/rest/services/',
              required=True)
@click.option(
    '--sr', '-s',
    help='srs EPSG ID. For example, 2435',
    type=int,
    default=2435,
    required=False)
@click.option(
    '--start-service',
    help='Start loop service number. The default is 0',
    type=int,
    default=0,
    required=False)
@click.option(
    '--start-layer',
    help='Start loop layer number. The default is 0',
    type=int,
    default=0,
    required=False)
@click.option(
    '--end-service',
    help='End loop service number, The default is -1 means to the last one.',
    type=int,
    default=-1,
    required=False)
@click.option(
    '--end-layer',
    help='End loop layer number. The default is -1 means to the last one',
    type=int,
    default=-1,
    required=False)
@click.option(
    '--output-path', '-o',
    help='Output file geodatabase, need the full path. For example, res/data.gdb',
    required=True)
def main(service_url, sr, start_service, start_layer, end_service, end_layer, output_path):
    # service_url = "http://suplicmap.pnr.sz/dynaszmap_1/rest/services"
    if service_url[-1] == r"/":
        service_url_json = service_url[:-1] + "?f=pjson"
    else:
        service_url_json = service_url + "?f=pjson"

    respData = get_json(service_url_json)

    json_info = json.loads(respData)
    services = json_info['services']

    if end_service == -1:
        end_service = len(services)
    else:
        end_service = end_service + 1

    for i in range(start_service, end_service):
        service = services[i]
        serviceName = service["name"]
        mapservice_url = service_url + "/" + service["name"] + "/" + service["type"]
        respData = get_json(mapservice_url + "?f=pjson")

        json_layer = json.loads(respData)
        layers = json_layer['layers']

        if i > start_service:
            start_layer = 0
        if i < end_service - 1:
            end_layer2 = len(layers)
        elif i == end_service - 1:
            end_layer2 = end_layer
        elif end_layer == -1:
            end_layer2 = len(layers)
        else:
            end_layer2 = len(layers)

        for j in range(start_layer, end_layer2):
            layer = layers[j]
            layerName = layer['name']
            url = mapservice_url + "/" + str(layer['id'])
            log.info(f'正在抓取{serviceName}服务({i})的{layerName}图层({j}).{url}')
            if not crawl(url=url, layer_name=None, sr=sr, loop_pos=-1, output_path=output_path, service_name=serviceName, layer_order=j):
                log.error("抓取失败!")
                continue


class crawlError(Exception):
    pass


if __name__ == '__main__':
    main()


