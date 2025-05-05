import json
import os
import pandas as pd


#Loads in raw API data from the Instagram platform. 
def ingest_instagram_data(path):
    """
    Loads in raw API data from the Instagram platform to then be processed and 
    turned into a dataframe for further analysis

    Parameters:
    path (str): File path to raw csv file

    Return:
    DataFrame of the raw data we collected from the API
    """

    #Define list to hold all Instagram posts
    instagram_post_collection = []
    instagram_entries = os.listdir(path)

    for entry in instagram_entries:
        #Create file paths with ease, regardless of os
        file_path = os.path.join(path, entry)

        #Open and load the json files
        with open(file_path, 'r') as file:
            try: 
                #Get dictionaries that we will then store in the collection list
                data = json.load(file)
                post_collection = data.get("data", {}).get("posts", [])

                for post in post_collection: 
                    #Since node is present for each example, we will be entering 
                    #via node
                    node = post.get("node", {})
                    typename = node.get("__typename")
                    display_url = node.get("display_url")
                    is_video = node.get("is_video")
                    comment_count = node.get("edge_media_to_comment", {}).get("count")
                    is_affiliate = node.get("is_affiliate")
                    is_paid_partnership = node.get("is_paid_partnership")
                    comments_disabled = node.get("comments_disabled")
                    edge_media_to_caption = node.get("edge_media_to_captain", {}).get("edges", [])
                    like_count = node.get("edge_media_preview_like", {}).get("count")
                    owner_id = node.get("owner", {}).get("id")
                    owner_user = node.get("owner", {}).get("username")
                    location = node.get("location")
                    thumbnail_src = node.get("thumbnail_src")

                    #Append each post to the post_collection list
                    instagram_post_collection.append({
                        "media": "Instagram",
                        "typename": typename,
                        "display_url": display_url,
                        "is_video": is_video,
                        "comment_count": comment_count,
                        "is_affiliate": is_affiliate,
                        "is_paid_partnership": is_paid_partnership,
                        "comments_disabled": comments_disabled,
                        "edge_media_to_caption": edge_media_to_caption,
                        "like_count": like_count,
                        "owner_id": owner_id,
                        "owner_user": owner_user, 
                        "location": location,
                        "thumbnail_src": thumbnail_src
                    })

            except Exception as e:
                print(f'We could not process this file due to {e}.') 

    return pd.DataFrame(instagram_post_collection)


#Function to run the program (followed by name-main section)
def main():
    instagram_folder = "./ensemble_data/instagram_data"
    df = ingest_instagram_data(instagram_folder)
    df.to_csv("cleaned_instagram_data.csv", index=False)

if __name__ == "__main__":
    main()