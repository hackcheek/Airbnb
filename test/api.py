import src.api as api

url = api.url
params = api.params

if __name__ == '__main__':
    params['rows'] = 10
    status, content = api.GET(url, params=params)
    with open('datasets/test.json', 'w') as file:
        file.write(content)
    print(f'status: {status}')
