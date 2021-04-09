from comprehend import ComprehendDetect
import time
import boto3
import urllib.request, json 
import random
import youtube_dl

def parse_time(seconds):
    parsed_time = seconds
    if seconds < 60:
        return parsed_time
    elif (seconds > 60) and (seconds < 60*60):
        return parsed_time/60
    else:
        return seconds/3600

def audio_to_text(audio_uri, lang, region, access_key, secret_key):
    start = time.time()
    transcribe = boto3.client(
        'transcribe', 
        region_name = region,
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key
    )
    job_name = "test"+str(random.randint(0,1000))
    
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': audio_uri},
        MediaFormat='mp3',
        LanguageCode=lang
    )
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        print(status)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
    end = time.time()    

    url_path = status["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    with urllib.request.urlopen(url_path) as url:
        data = json.loads(url.read().decode())
        
    result = {"executedTime": parse_time(end - start), "data": data}    
    
    return result
    
def youtube_to_mp3(video_url, output_filename):        
    
    ydl_opts = {
        "outtmpl": output_filename,
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

def upload_to_aws(local_file, bucket, s3_file, access_key, secret_key):
    s3 = boto3.client('s3', aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)

    s3.upload_file(local_file, bucket, s3_file)


def sentiment_analysis(text, size, region, access_key, secret_key):
     
    comp_detect = ComprehendDetect(boto3.client(
        'comprehend', 
        region_name = region,
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key
    ))                
    languages = comp_detect.detect_languages(text)
    lang_code = languages[0]['LanguageCode']  
    entities = comp_detect.detect_entities(text, lang_code)
    phrases = comp_detect.detect_key_phrases(text, lang_code)
    sentiment = comp_detect.detect_sentiment(text, lang_code)
    result = {"entities": entities[:size], "key_phrases": phrases[:size], "sentiment":sentiment}
    return result

def main(video_url, output_name, size, lang, bucket, region, access_key, secret_key):
    # Primero se descarga a mp el video desde el link de Youtube
    print(f"Dowload video {video_url}")
    print("#"*10)
    youtube_to_mp3(video_url, output_name)
    #video_title = get_title(video_url)
    # Despues se sube a s3 de AWS    
    s3_file = f"Data/{output_name}"
    print(f"Upload to S3 {s3_file}")
    print("#"*10)
    upload_to_aws(output_name, bucket, s3_file, access_key, secret_key)
    # Ahora se lee el mp3 de s3 para pasarlo a texto
    audio_uri = f"s3://{bucket}/{s3_file}"
    print(f"S3 audio to text {audio_uri}")
    print("#"*10)
    text_result = audio_to_text(audio_uri, lang, region, access_key, secret_key)
    # Finalmente se hace el analisis de sentimiento
    result = sentiment_analysis(text_result, size, region, access_key, secret_key)
    return result

bucket = 'XXXXXXXXXXXXXXXXXXXXXXXXXX'
region = "sa-east-1"
ACCESS_KEY_ID = 'XXXXXXXXXXXXXXXXXXXXXXXXXX'
SECRET_ACCESS_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXX'

main('https://www.youtube.com/watch?v=wCyNbKTqew8&ab_channel=ESPNFans', 
     "test_audio.mp3", 5, "es-ES", 
     bucket, region, ACCESS_KEY_ID, SECRET_ACCESS_KEY)    