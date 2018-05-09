from datetime import datetime as dt
from time import sleep

import vk_api
from airtable import Airtable

friends = []


def at_auth(base_key, table_name, api_key):
    return Airtable(base_key, table_name, api_key=api_key)


def vk_auth(login, password):
    vk = vk_api.VkApi(login=login, password=password)
    vk.auth()
    return vk


def new_friend_added(vk):
    try:
        new_friend = vk.method('friends.getRecent', {'count': 1})[0]
    except IndexError:
        return None
    if new_friend not in friends:
        friends.append(new_friend)
        return new_friend


def get_new_friend_info(vk, new_friend):
    info = {}
    info['add_time'] = dt.now().strftime('%d.%m.%Y %H:%M')
    resp = vk.method(
        'users.get', {
            'user_ids': new_friend, 'fields': 'contacts, bdate, city'
            })[0]
    info['name'] = '{} {}'.format(resp['first_name'], resp['last_name'])
    info['link'] = 'vk.com/id{}'.format(resp['id'])
    try:
        if resp['mobile_phone'] == '' or not all(n.isnumeric() for n in resp['mobile_phone']):
            info['mobile_phone'] = 'Не указан'
        else:
            info['mobile_phone'] = resp['mobile_phone']
        if resp['home_phone'] == '' or not all(n.isnumeric() for n in resp['home_phone']):
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
    airtable.insert({
        'Имя': info['name'],
        'Ссылка на профиль': info['link'],
        'Мобильный телефон': info['mobile_phone'],
        'Домашний телефон': info['home_phone'],
        'Дата рождения': info['bdate'],
        'Город': info['city'],
        'Дата добавления': info['add_time']
    })


def main():
    vk = vk_auth('zvyagdenis@gmail.com', '8ed0faf2')
    airtable = at_auth('appJRVy4Zj1DEmS29', 'New friends', 'keytbJxjeLWqOnrWD')
    while True:
        sleep(1)
        new_friend = new_friend_added(vk)
        if(new_friend):
            push_vk_info_to_at(airtable, get_new_friend_info(vk, new_friend))


if __name__ == '__main__':
    main()