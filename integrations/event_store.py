"""
    Sub-module to consume event from the Event Store stream
"""

# IMPROVEMENTS
# In a real world scenario this would probably run outside the batch ETL as a service pulling the events into file and
# pushing files to S3 at a set time interval. From my understanding of Event Store, a ETL would be more likely to
# be pulling consolidated data from projections that I understand are stored in MongoDB at Digital Risks.
#
# PhotonPump was chosen to this implementation because it communicates with the Event Store via TCP what is supposed to
# to be faster then HTTP. The library was also better documented than AttomicPuppy and AttomicPuppy was not handling
# correctly reaching the last event available in the stream (it was crashing with a permission error)
#
# This sub-module should be re-written after understanding better how Event Store is used.

import asyncio
import json
import logging
from photonpump import connect

log = logging.getLogger(name=__name__)


async def extract_events_from_stream(connection_parameters, stream, output_file):
    """
    Consumes event stream as a batch process and stores results in a json file

    :param connection_parameters: parameters to connect to the Event Store (dict)
    :param stream: stream name (str)
    :param output_file:  file to save json objects (str)
    :return: none
    """
    with open(output_file, 'w') as out_file:
        async with connect(
                host=connection_parameters['tcp_host'],
                port=connection_parameters['tcp_port'],
                username=connection_parameters['username'],
                password=connection_parameters['password']
        ) as conn:
            events_counter = 0
            errors_counter = 0
            async for event in conn.iter(stream):
                events_counter += 1
                try:
                    event_json = event.json()
                    # Enrich event with stream metadata
                    event_json['streamMetadata'] = {}
                    event_json['streamMetadata']['eventId'] = str(event.id)
                    event_json['streamMetadata']['eventType'] = str(event.type)
                    event_json['streamMetadata']['eventStream'] = str(event.stream)
                    event_json['streamMetadata']['eventCreated'] = str(event.created)
                    out_file.write(json.dumps(event_json) + '\n')
                except json.decoder.JSONDecodeError:
                    errors_counter += 1

    if errors_counter > 0:
        log.warning(f'Data loss alert: {errors_counter} events out of {events_counter} failed to encode to JSON')
    log.info(f'Extracted {int(events_counter - errors_counter)} events from stream {stream}')
