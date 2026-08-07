# Synthetic browser fixtures

These databases contain invented domains, users, titles, and timestamps only. No case
evidence is included.

- `.../Google/Chrome/User Data/Default/History` is created by
  `tests/generate_fixture.py` with one `urls` row joined to one `visits` row and an
  empty `downloads` table. It supports the #136 empty-output regression and the #137
  exact Chromium history assertions.
- `.../Mozilla/Firefox/Profiles/synthetic.default-release/places.sqlite` is created by
  the same script with five places, four history visits, four type-1 bookmarks, and
  seven type-2 bookmark folders. The fifth place has `visit_count=0` and deliberately
  has no `moz_historyvisits` row.

Regenerate both fixtures from the repository root with:

```bash
python tests/generate_fixture.py
```

The script deletes and recreates each SQLite database deterministically.
