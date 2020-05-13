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
    help='Start loop service. For example, 0',
    type=int,
    default=0,
    required=False)
@click.option(
    '--start-layer',
    help='Start loop layer. For example, 0',
    type=int,
    default=0,
    required=False)
@click.option(
    '--output-path', '-o',
    help='Output file geodatabase, need the full path. For example, res/data.gdb',
    required=True)
def main(service_url, sr, start_service, start_layer, output_path):
    # service_url = "http://suplicmap.pnr.sz/dynaszmap_1/rest/services"
    if service_url[-1] == r"/":
        service_url_json = service_url[:-1] + "?f=pjson"
    else:
        service_url_json = service_url + "?f=pjson"

    respData = get_json(service_url_json)

    json_info = json.loads(respData)
    services = json_info['services']

    for i in range(start_service, len(services)):
        service = services[i]
        serviceName = service["name"]
        mapservice_url = service_url + "/" + service["name"] + "/" + service["type"]
        respData = get_json(mapservice_url + "?f=pjson")

        json_layer = json.loads(respData)
        layers = json_layer['layers']

        if i > start_service:
            start_layer = 0

        for j in range(start_layer, len(layers)):
            try:
                layer = layers[j]
                layerName = layer['name']
                url = mapservice_url + "/" + str(layer['id'])
                log.info(f'正在爬取{serviceName}服务({i})的{layerName}图层({j}).{url}')
                if not crawl(url=url, layer_name=None, sr=sr, loop_pos=-1, output_path=output_path):
                    raise Exception()
            except Exception as err:
                log.error("爬取失败!" + traceback.format_exc())
                continue


if __name__ == '__main__':
    main()


