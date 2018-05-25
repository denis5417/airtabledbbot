from datetime import datetime as dt
from time import sleep

import vk_api
from airtable import Airtable

friends = {}


def at_auth(base_key, table_name, api_key):
    return Airtable(base_key, table_name, api_key=api_key)


def vk_auth(login, password):
    vk = vk_api.VkApi(login=login, password=password)
    vk.auth()
    return vk


def parse_table(airtable):
    friends = {}
    for page in airtable.get_iter():
        for record in page:
            friends[int(record['fields']['Ссылка на профиль'].replace(
                'vk.com/id', ''))] = record['id']
    return friends


def get_friend_ids(vk):
    return vk.method('friends.get')['items']


def update_friends(vk, at, friend_ids, friends):
    for id in friend_ids:
        try:
            diff = get_diff(get_friend_info(vk, id),
                            at.get(friends[id])['fields'])
            if diff:
                at.update(friends[id], diff)
        except KeyError:
            friends[id] = push_vk_info_to_at(at, get_friend_info(vk, id))
        except TypeError:
            pass


def get_diff(vk_info, at_info):
    diff = {}
    if vk_info['name'] != at_info['Имя']:
        diff['Имя'] = vk_info['name']
    if vk_info['mobile_phone'] != at_info['Мобильный телефон']:
        diff['Мобильный телефон'] = vk_info['mobile_phone']
    if vk_info['home_phone'] != at_info['Домашний телефон']:
        diff['Домашний телефон'] = vk_info['home_phone']
    if vk_info['bdate'] != at_info['Дата рождения']:
        diff['Дата рождения'] = vk_info['bdate']
    if vk_info['city'] != at_info['Город']:
        diff['Город'] = vk_info['city']
    return diff


def get_friend_info(vk, new_friend):
    info = {}
    info['add_time'] = dt.now().strftime('%d.%m.%Y %H:%M')
    resp = vk.method(
        'users.get', {
            'user_ids': new_friend, 'fields': 'contacts, bdate, city'
        })[0]
    info['name'] = '{} {}'.format(resp['first_name'], resp['last_name'])
    info['link'] = 'vk.com/id{}'.format(resp['id'])
    try:
        if resp['mobile_phone'] == '' or not all(
                n.isnumeric() for n in resp['mobile_phone']):
            info['mobile_phone'] = 'Не указан'
        else:
            info['mobile_phone'] = resp['mobile_phone']
        if resp['home_phone'] == '' or not all(
                n.isnumeric() for n in resp['home_phone']):
            info['home_phone'] = 'Не указан'
        else:
            info['home_phone'] = resp['home_phone']
    except KeyError:
        info['mobile_phone'] = 'Информация скрыта'
        info['home_phone'] = 'Информация скрыта'
    try:
        info['city'] = resp['city']['title']
    except KeyError:
        info['city'] = 'Не указан'
    try:
        bdate = dt.strptime(resp['bdate'], '%d.%m.%Y').strftime('%d.%m.%Y')
        info['bdate'] = bdate
    except KeyError:
        info['bdate'] = 'Не указана'
    except ValueError:
        bdate = dt.strptime(resp['bdate'], '%d.%m').strftime('%d.%m')
        info['bdate'] = bdate
    return info


def push_vk_info_to_at(airtable, info):
    return airtable.insert({
        'Имя': info['name'],
        'Ссылка на профиль': info['link'],
        'Мобильный телефон': info['mobile_phone'],
        'Домашний телефон': info['home_phone'],
        'Дата рождения': info['bdate'],
        'Город': info['city'],
        'Дата добавления': info['add_time']
    })


def main():
    vk = vk_auth('*login*', '*password*')
    airtable = at_auth('*base_id*', '*table_name*', '*api_key*')
    friends = parse_table(airtable)
    while True:
        update_friends(vk, airtable, get_friend_ids(vk), friends)


if __name__ == '__main__':
    main()
