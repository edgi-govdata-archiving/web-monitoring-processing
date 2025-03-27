from support import get_fixture_bytes
from web_monitoring.media import sniff_media_type


class TestSniffMediaType:
    def test_sniff_media_type_detects_html(self):
        raw_bytes = b'''
            <!doctype html public "-//w3c//dtd html 4.0 transitional//en">
            <html>
            <head>
                <title>Test Page</title>
            </head>
            <body>
                Test
            </body>
            </html>
        '''
        media = sniff_media_type(raw_bytes)
        assert media == 'text/html'

    def test_sniff_media_type_detects_pdf(self):
        raw_bytes = get_fixture_bytes('basic_title.pdf')
        media = sniff_media_type(raw_bytes)
        assert media == 'application/pdf'

    def test_sniff_media_type_uses_default_parameter_as_fallback(self):
        raw_bytes = b'This is just a random string'
        media = sniff_media_type(raw_bytes, 'my/default')
        assert media == 'my/default'
