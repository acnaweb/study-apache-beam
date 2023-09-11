from omegaconf import DictConfig


def build_dataflow_options(cfg : DictConfig):
    return  {
        "project": cfg.gcp.project,
        "runner": "DataflowRunner",
        "region": cfg.gcp.dataflow.region,
        "staging_location": cfg.gcp.dataflow.staging_location,
        "temp_location": cfg.gcp.temp_location,
        "template_location": f"{cfg.gcp.dataflow.template_location}/{cfg.job.name}",
        "save_main_session": cfg.gcp.dataflow.save_main_session,
        "subnetwork": cfg.gcp.dataflow.subnetwork,
        "streaming": cfg.gcp.dataflow.streaming,
        "max_num_workers": cfg.gcp.dataflow.max_num_workers,
        "setup_file": "./setup.py"
    }
