import yt_dlp

def extract_info(url, cookies):
    opts = {
        "quiet": True,
        "skip_download": True,
    }
    if cookies:
        opts["cookiefile"] = cookies

    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)