def get_domain_from_url(url):
    non_domain_set = {"secureemailportal", "zixportal"}
    patterns = [".com", ".org", ".edu", ".co"]
    for p in patterns:
        if p in url:
            url_split = url.split(p)
            first_part = url_split[0]
            url_splits = first_part.split(".")
            domain = url_splits[-1]
            if len(url_splits) >= 2:
                if url_splits[-1] in non_domain_set:
                    domain = url_splits[-2]
            return domain.split("/")[-1].lower()
    return None

url = "https://secureemail.propertyinfo.com/s/e?m=ABAjbdGHs2MOBoca70yPwpGp&em=processorassist%40uwm%2ecom"
print(get_domain_from_url(url))