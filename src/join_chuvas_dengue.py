import apache_beam as beam


def my_filter(element):
    return True


def build_pipeline_join_chuvas_dengue(chuvas, dengue):
    tag = "[join]/"

    result = (
        ({"chuvas": chuvas, "dengue": dengue})
        | f"{tag}Queue" >> beam.CoGroupByKey()  
        | f"{tag}Filter" >> beam.Filter(my_filter)
        | f"{tag}Print" >> beam.Map(print)        
    )    
    return result