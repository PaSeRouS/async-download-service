import aiofiles
import asyncio
import logging
import os
import pathlib

from aiohttp import web
from argparse import ArgumentParser
from asyncio import create_subprocess_exec, subprocess
from functools import partial

logger = logging.getLogger(__file__)


async def archive(request, throttle_tick_time, photos_dir):
    archive_hash = request.match_info['archive_hash']

    if not os.path.exists(os.path.join(photos_dir, archive_hash)):
        raise web.HTTPNotFound(text=f'404: Archive {archive_hash} does not exist.')

    response = web.StreamResponse(
        headers={
            'Content-Disposition': f'attachment; filename="{archive_hash}.zip"',
            'Content-Type': 'application/zip'
        }
    )

    await response.prepare(request)

    exec = 'zip'
    args = ['-r', '-', archive_hash]
    process = await create_subprocess_exec(exec, *args, stdout=subprocess.PIPE, cwd=photos_dir)

    try:
        while not process.stdout.at_eof():
            data = await process.stdout.read(512000)
            logger.info(f'Sending archive chunk {archive_hash}({len(data)})')

            if throttle_tick_time:
                await asyncio.sleep(throttle_tick_time)

            await response.write(data)
        
        await response.write_eof()
    except (web.HTTPRequestTimeout, ClientConnectionError, asyncio.CancelledError) as exc:
        logging.error('Download was interrupted: ', exc.text)
        raise exc
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()

            logger.info(f'Download was interrupted.')

    return response


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


def main():
    parser = ArgumentParser()
    parser.add_argument('-l', '--logs', action='store_true', help='Флаг для вкдючения логгирования')
    parser.add_argument('-t', '--throttle_tick', type=int, default=0, help='Количество секунд задержки ответа')
    parser.add_argument('-p', '--photo_dir', type=pathlib.Path, default='test_photos', help='Путь к архиву с фотографиями')
    args = parser.parse_args()

    logging.basicConfig(level=logging.ERROR)
    if args.logs:
        logger.setLevel(logging.INFO)

    if args.throttle_tick:
        throttle_tick_time = args.throttle_tick
    else:
        throttle_tick_time = 0
    
    if args.photo_dir:
        photos_dir = args.photo_dir
    else:
        photos_dir = 'test_photos'

    archive_handler = partial(
        archive,
        throttle_tick_time=throttle_tick_time,
        photos_dir=photos_dir
    )

    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', archive_handler),
    ])
    web.run_app(app)


if __name__ == '__main__':
    main()
