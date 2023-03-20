import argparse
import datetime
import pprint
import uuid
import time as ttime

from bluesky_kafka import Publisher, RemoteDispatcher
import nslsii.kafka_utils

from ophyd.utils.epics_pvs import data_shape, data_type
from event_model import compose_run

from tiled.client import from_profile



# SciAnalysis
########################################
import sys
SciAnalysis_PATH='/nsls2/data/cms/legacy/xf11bm/software/SciAnalysis/'
SciAnalysis_PATH in sys.path or sys.path.append(SciAnalysis_PATH)
#import glob
from SciAnalysis import tools
from SciAnalysis.XSAnalysis.Data import *
from SciAnalysis.XSAnalysis import Protocols
from SciAnalysis.Result import *

# Experimental parameters
########################################
calibration = Calibration(wavelength_A=0.9184) # 13.5 keV
calibration.set_image_size(1475, height=1679) # Pilatus2M
calibration.set_pixel_size(pixel_size_um=172.0)
calibration.set_beam_position(754, 1075)

calibration.set_distance(5.03) # 5m

mask_dir = SciAnalysis_PATH + '/SciAnalysis/XSAnalysis/masks/'
mask = Mask(mask_dir+'Dectris/Pilatus2M_gaps-mask.png')
mask.load('../../mask.png')

# Analysis to perform
########################################
#source_dir = '../../../raw/'
output_dir = '../../'


load_args = { 'calibration' : calibration, 
             'mask' : mask,
             #'background' : source_dir+'empty*saxs.tiff',
             #'transmission_int': '../../data/Transmission_output.csv', # Can also specify an float value.
             }
run_args = { 'verbosity' : 3,
            #'save_results' : ['xml', 'plots', 'txt', 'hdf5'],
            }

process = Protocols.ProcessorXS(load_args=load_args, run_args=run_args)
process.connect_databroker('cms') # Access databroker metadata

patterns = [
            ['theta', '.+_th(\d+\.\d+)_.+'] ,
            ['x_position', '.+_x(-?\d+\.\d+)_.+'] ,
            ['y_position', '.+_yy(-?\d+\.\d+)_.+'] ,
            #['anneal_time', '.+_anneal(\d+)_.+'] ,
            #['cost', '.+_Cost(\d+\.\d+)_.+'] ,
            ['annealing_temperature', '.+_T(\d+\.\d\d\d)C_.+'] ,
            #['annealing_time', '.+_(\d+\.\d)s_T.+'] ,
            #['annealing_temperature', '.+_localT(\d+\.\d)_.+'] ,
            #['annealing_time', '.+_clock(\d+\.\d\d)_.+'] ,
            #['o_position', '.+_opos(\d+\.\d+)_.+'] ,
            #['l_position', '.+_lpos(\d+\.\d+)_.+'] ,
            ['exposure_time', '.+_(\d+\.\d+)s_\d+_saxs.+'] ,
            ['sequence_ID', '.+_(\d+).+'] ,
            ]

protocols = [
    #Protocols.HDF5(save_results=['hdf5'])
    #Protocols.calibration_check(show=False, AgBH=True, q0=0.010, num_rings=4, ztrim=[0.05, 0.05], ) ,
    #Protocols.circular_average(ylog=True, plot_range=[0, 0.12, None, None], label_filename=True) ,
    #Protocols.thumbnails(crop=None, resize=1.0, blur=None, cmap=cmap_vge, ztrim=[0.01, 0.001]) ,
    
    #Protocols.circular_average_q2I_fit(show=False, q0=0.0140, qn_power=2.5, sigma=0.0008, plot_range=[0, 0.06, 0, None], fit_range=[0.008, 0.022]) ,
    Protocols.circular_average_q2I_fit(qn_power=3.5, trim_range=[0.005, 0.03], fit_range=[0.007, 0.019], q0=0.0120, sigma=0.0008) ,
    #Protocols.circular_average_q2I_fit(qn_power=3.0, trim_range=[0.005, 0.035], fit_range=[0.008, 0.03], q0=0.0180, sigma=0.001) ,
    
    #Protocols.databroker_extract(constraints={'measure_type':'measure'}, timestamp=True, sectino='start'),
    Protocols.metadata_extract(patterns=patterns) ,
    ]
    



# Helpers
########################################
from collections.abc import MutableMapping

def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str ='.') -> MutableMapping:
    # From:
    # https://www.freecodecamp.org/news/how-to-flatten-a-dictionary-in-python-in-4-different-ways/
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)






def reduce_run(bluesky_run):
    """
    Reduce data from a single bluesky run.

    Parameters
    ----------
    bluesky_run : BlueskyRun
        The run to be reduced, assumed to be a v2 run object.

    Returns
    -------
    reduced : dict
        A single event

    metadata : dict
        Will be top-level in the reduced start document
    """
    print(f"give reduced on run {bluesky_run}")
    reduced = {}
    reduced["next big thing"] = "avocado toast"



    # SciAnalysis code goes here
    ########################################
    verbosity = 3
    if verbosity>=3:
        print("Starting SciAnalysis analysis...")
    
    # Determine filename
    dir = bluesky_run.metadata['start']['experiment_alias_directory']
    filename = bluesky_run.metadata['start']['filename']
    infile = '{}saxs/raw/{}_saxs.tiff'.format(dir, filename)

    if verbosity>=3:
        print(f"Running SciAnalysis on: {infile}")


    # Access raw data
    #detector_image = bluesky_run["primary"]["data"]["pilatus2M_image"].read()
    #print(detector_image)
    if False:
        uid = bluesky_run.metadata['start']['uid']
        h = process.get_db(uid=uid)
        detector_image = h.table(fill=True)["pilatus2M_image"].to_numpy()


    # Run SciAnalysis
    process.run([infile], protocols, output_dir=output_dir, force=True)
    results_dict = ResultsDB(source_dir=output_dir).extract_single(infile, verbosity=verbosity)
    value = results_dict['circular_average_q2I_fit']['fit_peaks_prefactor1']
    #error = results_dict['circular_average_q2I_fit']['fit_peaks_prefactor1_error']
    error = value*0.01
    variance = np.square(error)

    if verbosity>=4:
        print("SciAnalysis generated results_dict:")
        #print(results_dict)
        pprint.pprint(results_dict)

    # Flatten results_dict to put it into reduced dict
    results_dict = flatten_dict(results_dict, sep="__")
    reduced.update(results_dict)

    # for gpCAM
    n = 1.0e0 # TOCHANGE Rescale values for gpCAM, so that they are roughly of order unity (this avoids machine precision problems)
    reduced['value'] = value*n
    reduced['variance'] = variance*n
    reduced['analyzed'] = True
    
    # End of SciAnalysis specific code
    ########################################



    return reduced, {"raw_start": bluesky_run.metadata["start"]}


def publish_reduced_documents(reduced, metadata, reduced_publisher):
    cr = compose_run(metadata=metadata)
    reduced_publisher("start", cr.start_doc)

    desc_bundle = cr.compose_descriptor(
        name="primary",
        data_keys={
            k: {
                "dtype": data_type(v),
                "shape": data_shape(v),
                "source": "computed",
            }
            for k, v in reduced.items()
        },
    )

    reduced_publisher("descriptor", desc_bundle.descriptor_doc)
    t = ttime.time()
    reduced_publisher(
        "event",
        desc_bundle.compose_event(
            data=reduced,
            timestamps={k: t for k in reduced},
        ),
    )

    reduced_publisher("stop", cr.compose_stop())


def respond_to_stop_with_reduced(consumer_topic: str, testing: bool = False):

    kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

    cms_tiled_client = from_profile("cms")
    
    if testing:
        def output_reduced_document(name, doc):
            print(
                f"{datetime.datetime.now().isoformat()} output document: {name}\n"
                f"contents: {pprint.pformat(doc)}\n"
            )
    else:
        cms_sandbox_tiled_client = from_profile("cms_bluesky_sandbox")
        reduced_publisher = Publisher(
            key="",
            topic="cms.bluesky.reduced.documents",
            bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
            producer_config=kafka_config["runengine_producer_config"],
        )

        def output_reduced_document(name, doc):
            cms_sandbox_tiled_client.v1.insert(name, doc)
            reduced_publisher(name, doc)

    def on_stop_reduce_run(name, doc):
        print(
            f"{datetime.datetime.now().isoformat()} document: {name}\n"
            f"contents: {pprint.pformat(doc)}\n"
        )
        if name == "stop":
            # look up the results of this run
            run_start_id = doc["run_start"]
            print(f"found run_start id {run_start_id}")
            bluesky_run = cms_tiled_client[run_start_id]
            reduced, metadata = reduce_run(bluesky_run)
            publish_reduced_documents(reduced, metadata, output_reduced_document)
        else:
            pass

    # this consumer should not be in a group with other consumers
    #   so generate a unique consumer group id for it
    unique_group_id = f"reduce-{str(uuid.uuid4())[:8]}"

    kafka_dispatcher = RemoteDispatcher(
        topics=[consumer_topic],
        bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
        group_id=unique_group_id,
        consumer_config=kafka_config["runengine_producer_config"],
    )

    kafka_dispatcher.subscribe(on_stop_reduce_run)
    kafka_dispatcher.start()


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--consumer-topic",
        default="cms.bluesky.runengine.documents",
        help="Kafka topic for reduction_agent input",
    )

    parser.add_argument(
        "--testing",
        default=False,
        action="store_true",
        help="reduction_agent will send output only to the console"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    print('Starting respond_to_stop_with_reduced loop...')
    respond_to_stop_with_reduced(**vars(args))
