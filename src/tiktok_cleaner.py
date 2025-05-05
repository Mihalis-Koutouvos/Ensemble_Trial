import json
import os
import pandas as pd


#Loads in raw API data from the TikTok platform. 
def ingest_tiktok_data(path):
    """
    Loads in raw API data from the TikTok platform to then be processed and 
    turned into a dataframe for further analysis

    Parameters:
    path (str): File path to raw csv file

    Return:
    DataFrame of the raw data we collected from the API
    """
    #Define list to hold all TikTok posts
    tiktok_post_collection = []
    tiktok_entries = os.listdir(path)

    for entry in tiktok_entries:
        #Create file paths with ease, regardless of os
        file_path = os.path.join(path, entry)

        #Open and load the json files
        with open(file_path, 'r') as file:
            try: 
                #Get dictionaries that we will then store in the collection list
                data = json.load(file)
                post_collection = data.get("data", [])

                for post in post_collection: 
                    ai_info = post.get("aigc_info", {}).get("created_by_ai")
                    verified = post.get("author", {}).get("enterprise_verify_reason")
                    follower_count = post.get("author", {}).get("follower_count")
                    following_count = post.get("author", {}).get("following_count")
                    language = post.get("author", {}).get("language")
                    region = post.get("author", {}).get("region")
                    description = post.get("desc")
                    description_lang = post.get("desc_language")
                    duration = post.get("duration")
                    allow_adding_to_story = post.get("interact_permission", {}).get("allow_adding_to_story")
                    is_ads = post.get("is_ads")
                    is_story = post.get("is_story")
                    long_video = post.get("long_video")
                    music_album = post.get("music", {}).get("album")
                    music_author = post.get("music", {}).get("author")
                    collect_count = post.get("statistics", {}).get("collect_count")
                    comment_count = post.get("statistics", {}).get("comment_count")
                    digg_count = post.get("statistics", {}).get("digg_count")
                    download_count = post.get("statistics", {}).get("download_count")
                    play_count = post.get("statistics", {}).get("play_count")
                    repost_count = post.get("statistics", {}).get("repost_count")
                    share_count = post.get("statistics", {}).get("share_count")
                    allow_comment = post.get("status", {}).get("allow_comment")
                    allow_share = post.get("status", {}).get("allow_share")
                    is_private = post.get("status", {}).get("is_private")
                    is_prohibited = post.get("status", {}).get("is_prohibited")
                    hashtags = [icon.get("hashtag_name") for icon in post.get("text_extra", []) if icon.get("hashtag_name")]
                    allow_download = post.get("video_control", {}).get("allow_download")
                    allow_react = post.get("video_control", {}).get("allow_react")
                    promotional_music = post.get("with_promotional_music")
                    watermark = post.get("without_watermark")

                    #Append each post to the post_collection list
                    tiktok_post_collection.append({
                        "media": "TikTok",
                        "created_by_ai": ai_info,
                        "enterprise_verify_reason": verified,
                        "follower_count": follower_count,
                        "following_count": following_count,
                        "language": language,
                        "region": region,
                        "desc": description,
                        "desc_language": description_lang,
                        "duration": duration,
                        "allow_adding_to_story": allow_adding_to_story,
                        "is_ads": is_ads,
                        "is_story": is_story,
                        "long_video": long_video,
                        "album": music_album,
                        "author": music_author,
                        "collect_count": collect_count,
                        "comment_count": comment_count,
                        "digg_count": digg_count,
                        "download_count": download_count,
                        "play_count": play_count,
                        "repost_count": repost_count,
                        "share_count": share_count,
                        "allow_comment": allow_comment,
                        "allow_share": allow_share,
                        "is_private": is_private,
                        "is_prohibited": is_prohibited,
                        "hashtag_name": hashtags,
                        "hashtag_count": len(hashtags),
                        "allow_download": allow_download,
                        "allow_react": allow_react,
                        "with_promotional_music": promotional_music,
                        "without_watermark": watermark
                    })


        
            except Exception as e:
                print(f'We could not process this file due to {e}.') 



    return pd.DataFrame(tiktok_post_collection)


#Function to run the program (followed by name-main section)
def main():
    tiktok_folder = "./ensemble_data/tiktok_data"
    df = ingest_tiktok_data(tiktok_folder)
    df.to_csv("cleaned_tiktok_data.csv", index=False)

if __name__ == "__main__":
    main()