import re
from datetime import datetime as dt
from time import sleep, time

import requests
import vk_api
from airtable import Airtable


def at_auth(base_key, table_name, api_key):
    return Airtable(base_key, table_name, api_key=api_key)


def vk_auth(login, password):
    vk = vk_api.VkApi(login=login, password=password)
    vk.auth()
    return vk


def parse_table(airtable, vk):
    friends = {}
    for record in airtable.get_all(view='Таблица Денис'):
        if 'Профиль в соцсети' not in record['fields']:
            continue
        vk_id = re.search(
            r'(?<=vk.com/)[0-9a-zA-Z_.]*',
            record['fields']['Профиль в соцсети'])
        if vk_id is None:
            continue
        else:
            vk_id = vk_id[0]
        if vk_id == 'im':
            vk_id = re.search(
                r'(?<=sel=)[0-9a-zA-Z_.]*',
                record['fields']['Профиль в соцсети'])[0]
        try:
            vk_id = vk.method(
                'users.get', {
                    'user_ids': vk_id, 'fields': '',
                    'lang': 'ru'
                })[0]['id']
            friends[vk_id] = record['id']
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.HTTPError,
                vk_api.exceptions.ApiHttpError,
                vk_api.exceptions.ApiError):
            pass
    friends[vk_id] = record['id']
    return friends


def get_all_friend_ids(vk):
    try:
        return vk.method('friends.get', {'count': 100000})['items']
    except (requests.exceptions.ReadTimeout, requests.exceptions.HTTPError,
            vk_api.exceptions.ApiHttpError) as err:
        print(err)


def update_friends(vk, at, friend_ids, friends):
    if friend_ids is not None:
        for id in friend_ids:
            if id not in friends:
                friends[id] = push_vk_info_to_at(at, get_friend_info(vk, id))
    for id in friends:
        try:
            vk_info = get_friend_info(vk, id)
            at_info = at.get(friends[id])
        except (requests.exceptions.ReadTimeout, requests.exceptions.HTTPError,
                vk_api.exceptions.ApiHttpError) as err:
            print(err)
            continue
        if vk_info and at_info:
            diff = get_diff(vk_info, at_info['fields'])
        if diff:
            try:
                at.update(friends[id], diff)
            except (requests.exceptions.HTTPError,
                    requests.exceptions.ReadTimeout) as err:
                print(err)


def check_info(field, at_info, vk_field):
    return vk_field is not None \
        and ((field not in at_info or
              vk_field != at_info[field]) and vk_field is not None)


def get_diff(vk_info, at_info):
    diff = {}
    check_info('ФИО', at_info, vk_info['name'])
    if 'name' in vk_info and check_info('ФИО', at_info, vk_info['name']):
        diff['ФИО'] = vk_info['name']
    if 'phone' in vk_info and check_info('Телефон', at_info, vk_info['phone']):
        diff['Телефон'] = vk_info['phone']
    if 'bdate' in vk_info and \
            check_info('Дата рождения', at_info, vk_info['bdate']):
        diff['Дата рождения'] = vk_info['bdate']
    if 'bdate_age' in vk_info and \
            check_info('Возраст', at_info, vk_info['bdate_age']):
        diff['Возраст'] = vk_info['bdate_age']
    if 'city' in vk_info and check_info('Город', at_info, vk_info['city']):
        diff['Город'] = vk_info['city']
    return diff


def get_friend_info(vk, new_friend):
    info = {}
    try:
        resp = vk.method(
            'users.get', {
                'user_ids': new_friend, 'fields': 'contacts, bdate, city',
                'lang': 'ru'
            })[0]
    except (requests.exceptions.ReadTimeout, requests.exceptions.HTTPError,
            vk_api.exceptions.ApiHttpError) as err:
        print(err)
        info['name'] = None
        info['link'] = None
        info['phone'] = None
        info['city'] = None
        info['bdate'] = None
        return info
    info['name'] = '{} {}'.format(resp['first_name'], resp['last_name'])
    info['link'] = 'vk.com/id{}'.format(resp['id'])
    info['phone'] = ''
    if 'mobile_phone' in resp and all(
            n.isnumeric() for n in resp['mobile_phone']):
        info['phone'] += resp['mobile_phone'] + ' '
    if 'home_phone' in resp and all(
            n.isnumeric() for n in resp['home_phone']):
        info['phone'] += resp['home_phone']
    if info['phone'].strip() == '':
        info['phone'] = None
    info['city'] = resp['city']['title'] if 'city' in resp else None
    if 'bdate' not in resp:
        info['bdate'] = None
        info['bdate_age'] = None
        return info
    try:
        resp['bdate'] = resp['bdate'].replace('29.2', '28.2')
        bdate = dt.strptime(resp['bdate'], '%d.%m.%Y').strftime(
            '%-m.%d') + '.' + str(dt.now().year)
        info['bdate'] = bdate
        bdate_age = dt.strptime(
            resp['bdate'], '%d.%m.%Y').strftime('%-m.%d.%Y')
        info['bdate_age'] = bdate_age
    except ValueError:
        bdate = dt.strptime(resp['bdate'], '%d.%m').strftime(
            '%-m.%d') + '.' + str(dt.now().year)
        info['bdate'] = bdate
        info['bdate_age'] = None
    return info


def push_vk_info_to_at(airtable, info):
    return airtable.insert({
        'ФИО': info['name'],
        'Источник': 'ВК Высота',
        'Профиль в соцсети': info['link'],
        'Телефон': info['phone'],
        'Дата рождения': info['bdate'],
        'Город': info['city'],
    }, typecast=True)['id']


def main():
    vk = vk_auth(
        '', '')
    airtable = at_auth('',
                       '', '')
    friends = parse_table(airtable, vk)
    print('Table parsed')
    while True:
        update_friends(vk, airtable, get_all_friend_ids(vk), friends)


if __name__ == '__main__':
    main()
