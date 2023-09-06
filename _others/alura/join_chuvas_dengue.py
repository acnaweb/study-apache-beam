import apache_beam as beam


def filter_empty(element):
    key, dados = element

    if all([
            dados["chuvas"], 
            dados["dengue"]
           ]):
        return True

    return False


def descompact(element):
    key, dados = element

    chuva = dados["chuvas"][0]
    dengue = dados["dengue"][0]
    uf, ano, mes  = key.split("-")

    return (uf, ano, mes, str(chuva), str(dengue))

def build_pipeline_join_chuvas_dengue(chuvas, dengue):
    tag = "[join]/"

    result = (
        ({"chuvas": chuvas, "dengue": dengue})
        | f"{tag}Queue" >> beam.CoGroupByKey()  
        | f"{tag}Filter" >> beam.Filter(filter_empty)
        | f"{tag}Descompactar" >> beam.Map(descompact)
        #| f"{tag}Print" >> beam.Map(print)        
    )    
    return result