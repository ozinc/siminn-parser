#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from __future__ import print_function
import os
import sys
import re
import logging
import argparse
import json
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

# Helper functions

def format(timestamp):
    return timestamp.format('YYYY-MM-DDTHH:mm:ss.SSS') + 'Z'

# EPG importer

def import_epg():
    log.info('importing EPG for channel: skjarinn')
    stdin = sys.stdin.buffer.read()
    soup = bs.BeautifulSoup(stdin, 'xml', from_encoding='iso-8859-1')
    events = soup.findAll('event')
    log.info('found %d scheduled items', len(events))

    # Fetch the primary stream for this channel
    channel_id = api.channel_id
    stream = api.fetch_primary_stream_for_channel(channel_id)
    if stream == None:
        log.info('no stream found for {0} (id: {1})'.format(station, channel_id))
        sys.exit(-1)
    stream_id = stream['id']
    log.info('found streamId {0} for channel {1} (id: {2})'.format(
        stream_id, station, api.channel_id))

    # Start processing the EPG items
    for event in events:
        # If its older than today; ignore.
        # TODO: that!

        serie_id = event.recordid_efni.get('value')
        collection_id = None
        content_type = 'episode'
        has_episode_no = True

        # Parse the time strings
        start_time = arrow.get(event.get('starttime'))

        # Check if the event is associated with a collection
        if event.category:
            if event.category.get('value') == 'FRE':
                content_type = 'news'
                has_episode_no = False
            elif event.category.get('value') in MOVIE_CATEGORIES:
                content_type = 'movie'
                has_episode_no = False
            elif event.category.get('value') == 'MSE':
                content_type = 'movie'

        if content_type != 'movie':
            # Determine the name of the collection
            collection_name = event.title.text

            # Use the org_title (original title) field as collection name, if present.
            if event.org_title.text and event.org_title.text.strip():
                collection_name = event.org_title.text

            # Populate the collection object.
            collectionProps = {
                'externalId': serie_id,
                'type': 'series', # TODO: You shouldn't need to do this.
                'name': collection_name
            }

            collection = CoreObject('collection', collectionProps);
            collection_id = upsert_collection(collection)

        external_id = event.reference_number.get('value')
        external_video = api.fetch_video_by_external_id(external_id)

        # Populate the metadata object.
        metadata = {}
        if len(event.description.text) > 0:
            metadata['description'] = h.unescape(event.description.text)

        # Populate the video object
        if has_episode_no:
            metadata['episodeNumber'] = int(event.series.get('episode'))
            if int(event.series.get('series_number')) > 0:
                metadata['seasonNumber'] = int(event.series.get('series_number'))

        if content_type == 'news':
            metadata['date'] = start_time.isoformat()

        # Playback regions
        playback_countries = DEFAULT_PLAYBACK_COUNTRIES
        if event.recordid_efni.get('value') in GLOBAL_EFNI:
            playback_countries = ['GLOBAL']
        if event.category.get('value') in GLOBAL_CATEGORIES:
            playback_countries = ['GLOBAL']

        # Moment control
        allow_moments = True
        if station in NO_MOMENTS_STATIONS:
            allow_moments = False
        elif event.recordid_efni.get('value') in NO_MOMENTS_EFNI:
            allow_moments = False

        availability_time = int(event.netdagar.get('value'))

        video_props = {}
        video_props['sourceType'] = 'stream'
        video_props['contentType'] = content_type
        video_props['title'] = h.unescape(event.title.text)
        video_props['externalId'] = event.reference_number.get('value')
        if content_type == 'movie' and station in STATION_TO_MOVIE_COLLECTION:
            video_props['collectionId'] = STATION_TO_MOVIE_COLLECTION[station]
        else:
            video_props['collectionId'] = collection_id
        video_props['published'] = True
        video_props['allowMoments'] = allow_moments
        video_props['playbackCountries'] = playback_countries

        if availability_time:
            video_props['playableUntil'] = format(start_time.replace(days=availability_time))

        if event.recordid_efni.get('value') in SERIES_UNPUBLISHED:
            video_props['published'] = False

        # Only attach the metadata field if we have some metadata.
        if len(metadata) > 0:
            video_props['metadata'] = metadata

        video = CoreObject('video', video_props)

        # Create/update the video:
        video_id = upsert_video(video)

        # Fetch external slot
        ext_slot_id = event.recid_syning.get('value')
        external_slot = api.fetch_slot_by_external_id(ext_slot_id)
        slot_props = {}
        slot_metadata = {}
        if external_slot is not None:
            slot_metadata = external_slot.get('metadata', {})

        # Determine the slot type:
        slot_type = 'regular'
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

        slot_props['type']       = slot_type
        slot_props['startTime']  = format(start_time)
        slot_props['metadata']   = slot_metadata
        slot_props['externalId'] = ext_slot_id

        # End time left empty as we want this slot to last until the next.
        slot_props['videoId']    = video_id

        # Associate the slot with the primary stream for this channel
        slot_props['streamId']   = stream_id

        # Create a slot to schedule the video to be played
        # at the specified time
        slot = CoreObject('slot', slot_props)
        upsert_slot(slot)

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
    import_epg()
    else:
        raise Exception('unsupported operation')
