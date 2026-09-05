/*
    Stremio stream picker

    A scrollable, selectable list of streams for a chosen movie/episode. Built
    on the same RecyclingGrid that the rest of the app uses (so it works with
    both touch and the D-pad). Selecting a row plays that stream.
*/
#pragma once

#include <borealis.hpp>
#include "api/stremio.hpp"
#include "activity/stremio_resume.hpp"

class RecyclingGrid;

class StreamPicker : public brls::Box {
public:
    // libraryItem is the title to add to the library when a stream is saved.
    // Leave it empty to save without touching the library.
    StreamPicker(const std::string& title, const std::vector<stremio::Stream>& streams, const ResumeEntry& resumeKey,
        stremio::Meta libraryItem = {});

private:
    RecyclingGrid* recycler = nullptr;
};
