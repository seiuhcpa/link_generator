# SEIUHCPA Prefilled Link Creator
Keeps the prefilled links of the active chapters for SEIUHCPA up to date.
* On chapter name changes redirects its shortlink to the new chapter name.
* Creates new prefilled links to sign up members for new chapters. (Called **main**)
* Creates derivatives of the **main** link for specific purposes (ex. nmo, cope)  
## New Chapter Links
While chapter name changes and derivatives links require no input to run. 
Creating the **main** link for new chapters requires that new chapter to have a *slashcode* (or short_link).
Chapters with active members that do not have a short link appear on [this](https://docs.google.com/spreadsheets/d/1MG6AxgghvMBFw-wWg1BgkPNy3AwYm3DZ-qfGhEJbSfc/edit#gid=0) sheet.
### Creating new links
Enter a shortcode into column I and if it is not a duplicate it will be added to the queue for new **main** prefilled links to create by the script *new_chapter_links*.

Then by running new_link_types its derivatives will also be created.
