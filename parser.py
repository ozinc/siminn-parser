#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function
import os
import sys
import re
import logging
import argparse
import json
from slugify import slugify
from collections import namedtuple
from html.parser import HTMLParser
h = HTMLParser()

import requests
import bs4 as bs
import arrow

from oz import OZCoreApi

username = os.environ['OZ_USERNAME']
password = os.environ['OZ_PASSWORD']

api = OZCoreApi(username, password)

# Logging setup
log = logging.getLogger(__name__)
log.setLevel(logging.WARN)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

CoreObject = namedtuple('CoreObject', ['type', 'properties'])

def get_primary_stream(station, channel_id):
    stream = api.fetch_primary_stream_for_channel(channel_id)
    if stream == None:
        log.info('no stream found for {0} (id: {1})'.format(station, channel_id))
        sys.exit(-1)
    log.info('found streamId {0} for channel {1} (id: {2})'.format(
        stream['id'], station, channel_id))
    return stream['id']

def is_zero_or_empty(s):
    return s == None or s == '' or s == '0'

def import_epg():
    station = 'siminn' # TODO: Make configurable
    channel_id = api.channel_id

    log.info('importing EPG for channel: {0}'.format(station))
    stdin = sys.stdin.buffer.read()
    soup = bs.BeautifulSoup(stdin, 'xml')
    events = soup.findAll('event')
    log.info('found %d scheduled items', len(events))

    stream_id = get_primary_stream(station, channel_id)

    # Start processing the EPG items
    for event in events:

        collection_id = None
        content_type = 'episode'

        # Parse start time and check if its older than a day, discard if so
        start_time = arrow.get(event.get('start-time'))
        now = arrow.utcnow()
        if (start_time - now).days > 1:
            sys.exit(-1)

        # Parse the episode metadata
        episode_info = event.episode
        #print(event.episode)
        if all(list(map(lambda s: is_zero_or_empty(event.episode[s]),
            ['number', 'number-of-episodes', 'series-number']))):
            content_type = 'movie'

        # Collection handling
        if content_type == 'episode':
            # Determine the name of the collection
            collection_name = event.title.text

            # Upsert the collection
            collection_props = {
                'externalId': 'siminn-' + slugify(collection_name),
                'type': 'series',
                'name': collection_name
            }
            print('upserting collection:', collection_props)
            #collection = CoreObject('collection', collection_props);
            #collection_id = upsert_collection(collection)

        # Video handling
        external_video_id = 'siminn-' + event.get('event-id')
        external_video = api.fetch_video_by_external_id(external_video_id)

        # Collect metadata
        metadata = {}
        if external_video is not None:
            metadata = external_video.get('metadata', {})

        if len(event.description.text) > 0:
            metadata['description'] = event.description.text

        if content_type == 'episode':
            if not is_zero_or_empty(event.episode['series-number']):
                metadata['episodeNumber'] = int(event.episode['number'])
            if not is_zero_or_empty(event.episode['series-number']):
                metadata['seasonNumber'] = int(event.episode['series-number'])

        print('video metadata:', metadata)

        # TODO: Playback regions
        # TODO: Availability

        video_props = {
            'sourceType': 'stream',
            'contentType': content_type,
            'title': event.title.text,
            'externalId': external_video_id,
            'collectionId': collection_id,
            'published': True,
            'metadata': metadata
        }

        #if availability_time:
        #    video_props['playableUntil'] = format(start_time.replace(days=availability_time))
        #if event.recordid_efni.get('value') in SERIES_UNPUBLISHED:
        #    video_props['published'] = False

        # Create/update the video:
        print('video:', video_props)
        # video = CoreObject('video', video_props)
        # video_id = upsert_video(video)
        video_id = 'banani'

        # Video handling
        external_slot_id = 'siminn-' + event.get('internal')
        external_slot = api.fetch_slot_by_external_id(external_slot_id)

        slot_metadata = {}
        if external_slot is not None:
            slot_metadata = external_slot.get('metadata', {})

        # Determine the slot type:
        slot_type = 'regular'
        '''
        if event.live.get('value') == 'true' and serie_id not in NOT_REALLY_LIVE:
            slot_type = 'live'
            log.info('slot type is LIVE')
        elif (event.premier.get('value') == 'true' and
             (metadata.get('episodeNumber') == 1 or content_type == 'movie')):
            slot_type = 'premiere'
            log.info('slot type is PREMIERE')

        tokens = event.get('duration').split(':')
        estimated_duration = int(tokens[0]) * 3600 + int(tokens[1]) * 60;
        slot_metadata['estimatedDuration'] = estimated_duration

        log.info('estimated duration {} => {}'.format(
            event.get('duration'), estimated_duration))
        '''

        slot_props = {
            'type': slot_type,
            'startTime': format(start_time),
            'metadata': slot_metadata,
            'externalId': external_slot_id,
            'videoId': video_id,
            'streamId': stream_id
        }

        print('slot:', slot_props)
        # Create a slot to schedule the video to be played
        # at the specified time
        #slot = CoreObject('slot', slot_props)
        #upsert_slot(slot)

# Helper functions

def format(timestamp):
    return timestamp.format('YYYY-MM-DDTHH:mm:ss.SSS') + 'Z'

def upsert_slot(slot, **kwargs):
    return upsert_external_object(slot, **kwargs)

def upsert_collection(collection, **kwargs):
    return upsert_external_object(collection, **kwargs)

def upsert_video(video, **kwargs):
    return upsert_external_object(video, **kwargs)

def upsert_external_object(obj,  **kwargs):
    external_obj = getattr(api, 'fetch_{}_by_external_id'.format(obj.type))(obj.properties['externalId'])
    if external_obj is None:
        log.info('creating {0}, obj: {1}'.format(obj.type, obj.properties))
        new_obj = getattr(api, 'create_{}'.format(obj.type))(obj.properties, **kwargs)
        return new_obj['id']
    else:
        # Anything changed? If not we don't want to do a PATCH request.
        def collect_diff(key, value):
            differs = not key in external_obj or external_obj[key] != value
            if differs:
                before = external_obj.get(key) if key in external_obj else None
                return { 'key': key, 'before': before, 'after': value }
            else:
                return None

        differences = []
        for k, v in obj.properties.items():
            diff = collect_diff(k, v)
            if diff:
                differences.append(diff)

        should_update = len(differences) > 0 or 'vodify' in kwargs

        # Upsert the object if something has changed.
        if should_update:
            if 'vodify' in kwargs:
                log.info('doing a PATCH because of vodification')
            log.debug('{0} had changed, diff: {1}'.format(obj.type, differences))
            obj.properties['id'] = external_obj['id']
            new_obj = getattr(api, 'update_{}'.format(obj.type))(obj.properties, **kwargs)
            return new_obj['id']
        else:
            log.info('not updating {0}, nothing has changed'.format(obj.type))
            return external_obj['id']

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='EPG importer for Skjarinn')
    parser.add_argument('-v', help='turn on verbose mode', action='store_true')
    parser.add_argument('channel', help='The ID of the channel being imported to')
    args = parser.parse_args()

    api.channel_id = args.channel
    if args.v:
        log.setLevel(logging.DEBUG)
        log.info('verbose mode on')

    # Lets do this!
    import_epg()
