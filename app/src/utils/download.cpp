#include "utils/download.hpp"
#include "utils/config.hpp"
#include <borealis/core/application.hpp>
#ifdef __SWITCH__
#include <switch.h>
#endif
#include "utils/thread.hpp"
#include "utils/misc.hpp"
#include "api/jellyfin.hpp"
#include "view/mpv_core.hpp"

std::string DownloadManager::downloadDir() const { return AppConfig::instance().configDir() + "/downloads"; }

void DownloadManager::init() {
    auto dir = this->downloadDir();
    if (!fs::exists(dir)) {
        fs::create_directories(dir);
    }
    this->loadIndex();

    std::lock_guard<std::mutex> lock(this->mutex);
    for (auto& item : this->items) {
        if (item.status == DownloadStatus::Downloading) {
            item.status = DownloadStatus::Queued;
        }
    }
    this->saveIndex();
}

void DownloadManager::loadIndex() {
    std::string path = this->downloadDir() + "/index.json";
    if (!fs::exists(path)) return;

    try {
        std::ifstream f(path);
        nlohmann::json j = nlohmann::json::parse(f);
        this->items = j.get<std::vector<DownloadItem>>();
    } catch (const std::exception& e) {
        brls::Logger::error("Failed to load download index: {}", e.what());
    }
}

void DownloadManager::saveIndex() {
    std::string path = this->downloadDir() + "/index.json";
    try {
        nlohmann::json j = this->items;
        std::ofstream f(path);
        f << j.dump(2);
    } catch (const std::exception& e) {
        brls::Logger::error("Failed to save download index: {}", e.what());
    }
}

void DownloadManager::addDownload(const std::string& itemId, DownloadQuality quality) {
    std::lock_guard<std::mutex> lock(this->mutex);

    for (auto& existing : this->items) {
        if (existing.itemId == itemId) {
            brls::Logger::info("Already exists: {}", itemId);
            return;
        }
    }

    jellyfin::getJSON<jellyfin::Episode>(
        [this, quality](const jellyfin::Episode& item) {
            std::lock_guard<std::mutex> lock(this->mutex);

            DownloadItem dl;
            dl.itemId = item.Id;
            dl.name = item.Name;
            dl.type = item.Type;
            dl.seriesName = item.SeriesName;
            dl.seasonIndex = item.ParentIndexNumber;
            dl.episodeIndex = item.IndexNumber;
            dl.productionYear = item.ProductionYear;
            dl.runTimeTicks = item.RunTimeTicks;
            dl.quality = quality;
            dl.status = DownloadStatus::Queued;
            if (item.SeriesId.is_string()) dl.seriesId = item.SeriesId.get<std::string>();
            for (auto& src : item.MediaSources) dl.filePath = src.Name;

            auto primaryTag = item.ImageTags.find(jellyfin::imageTypePrimary);
            if (primaryTag != item.ImageTags.end()) dl.imagePrimaryTag = primaryTag->second;

            this->items.push_back(dl);
            this->saveIndex();
            brls::Logger::info("Download queued: {}", item.Name);
            this->processQueue();
        },
        [](const std::string& ex) { brls::Application::notify(ex); }, jellyfin::apiUserItem,
        AppConfig::instance().getUserId(), itemId);
}

void DownloadManager::addStreamDownload(const std::string& itemId, const std::string& name,
    const std::string& type, const std::string& url, const std::string& fileName, const std::string& poster,
    int64_t totalBytes) {
    std::lock_guard<std::mutex> lock(this->mutex);

    for (auto& existing : this->items) {
        if (existing.itemId == itemId) {
            brls::Logger::info("Already exists: {}", itemId);
            return;
        }
    }

    // Unlike addDownload() there is nothing to fetch first: the caller already
    // holds everything (URL, filename, size, poster) from the stream listing.
    DownloadItem dl;
    dl.itemId = itemId;
    dl.name = name;
    dl.type = type;
    dl.sourceUrl = url;
    dl.posterUrl = poster;
    dl.filePath = fileName;
    dl.totalBytes = totalBytes;
    dl.quality = DownloadQuality::Original;
    dl.status = DownloadStatus::Queued;

    this->items.push_back(dl);
    this->saveIndex();
    brls::Logger::info("Stream download queued: {}", name);

    // Fetch the artwork now, while nothing else is in flight, rather than from
    // inside doDownload: the tile then shows up as soon as the item is queued,
    // and it is one small request on its own instead of a second one competing
    // with the transfer for the connection pool every HTTP object shares.
    if (!poster.empty()) {
        std::string dir = this->downloadDir() + "/" + itemId;
        std::string url = poster;
        brls::async([dir, url]() {
            try {
                if (!fs::exists(dir)) fs::create_directories(dir);
                HTTP::download(url, dir + "/thumb.png", HTTP::Timeout{10000});
            } catch (const std::exception& e) {
                brls::Logger::warning("Failed to fetch poster: {}", e.what());
            }
        });
    }

    this->processQueue();
}

void DownloadManager::resumeQueue() {
    std::lock_guard<std::mutex> lock(this->mutex);
    this->processQueue();
}

void DownloadManager::abortActive() {
    std::lock_guard<std::mutex> lock(this->mutex);
    if (this->currentCancel) this->currentCancel->store(true);
}

void DownloadManager::cancelDownload(const std::string& itemId) {
    bool erased = false;
    {
        std::lock_guard<std::mutex> lock(this->mutex);

        for (auto& item : this->items) {
            if (item.itemId == itemId && item.status == DownloadStatus::Downloading && this->currentCancel) {
                this->currentCancel->store(true);
                return;
            }
        }

        for (auto it = this->items.begin(); it != this->items.end(); ++it) {
            if (it->itemId == itemId && it->status == DownloadStatus::Queued) {
                this->items.erase(it);
                this->saveIndex();
                erased = true;
                break;
            }
        }
    }

    if (erased) {
        brls::sync([this, itemId]() {
            this->statusEvent.fire(itemId, DownloadStatus::Failed);
        });
    }
}

void DownloadManager::removeDownload(const std::string& itemId) {
    bool wasActive = false;
    bool erased = false;
    {
        std::lock_guard<std::mutex> lock(this->mutex);

        for (auto& item : this->items) {
            if (item.itemId == itemId && item.status == DownloadStatus::Downloading && this->currentCancel) {
                this->currentCancel->store(true);
                item.errorMessage = "removed";
                wasActive = true;
                break;
            }
        }

        if (!wasActive) {
            for (auto it = this->items.begin(); it != this->items.end(); ++it) {
                if (it->itemId == itemId) {
                    this->items.erase(it);
                    erased = true;
                    break;
                }
            }
            this->saveIndex();
        }
    }

    if (!wasActive) {
        std::string dir = this->downloadDir() + "/" + itemId;
        brls::async([dir]() {
            try {
                if (fs::exists(dir)) fs::remove_all(dir);
            } catch (const std::exception& e) {
                brls::Logger::error("Failed to remove download dir: {}", e.what());
            }
        });
    }

    if (erased) {
        brls::sync([this, itemId]() {
            this->statusEvent.fire(itemId, DownloadStatus::Failed);
        });
    }
}

DownloadStatus DownloadManager::findItem(const std::string& itemId) const {
    std::lock_guard<std::mutex> lock(this->mutex);
    for (auto& item : this->items) {
        if (item.itemId == itemId) return item.status;
    }
    return DownloadStatus::NotFound;
}

std::pair<size_t, size_t> DownloadManager::findSeries(const std::string& seriesId) const {
    std::lock_guard<std::mutex> lock(this->mutex);
    size_t count = 0, done = 0;
    for (auto& item : this->items) {
        if (item.seriesId == seriesId) {
            if (item.status == DownloadStatus::Completed) done++;
            ++count;
        }
    }
    return std::make_pair(count, done);
}

std::string DownloadManager::getLocalPath(const std::string& itemId) const {
    std::lock_guard<std::mutex> lock(this->mutex);
    for (auto& item : this->items) {
        if (item.itemId == itemId && item.status == DownloadStatus::Completed) {
            return this->downloadDir() + "/" + itemId + "/" + item.filePath;
        }
    }
    return "";
}

std::vector<DownloadItem> DownloadManager::getItems() const {
    std::lock_guard<std::mutex> lock(this->mutex);
    return this->items;
}

std::string DownloadManager::buildDownloadUrl(const DownloadItem& item) const {
    // A Stremio stream arrives with its final URL already resolved by the
    // addon -- there is no server + token to compose, and no quality variants
    // to request. Hand it straight back.
    if (!item.sourceUrl.empty()) return item.sourceUrl;

    auto& conf = AppConfig::instance();
    std::string server = conf.getUrl();
    std::string token = conf.getToken();

    switch (item.quality) {
    case DownloadQuality::Original:
        return server + fmt::format(fmt::runtime(jellyfin::apiDownload), item.itemId,
            HTTP::encode_form({{"api_key", token}}));
    case DownloadQuality::Q1080p:
        return server + fmt::format(fmt::runtime(jellyfin::apiStream), item.itemId,
            HTTP::encode_form({
                {"static", "false"},
                {"mediaSourceId", item.itemId},
                {"videoCodec", MPVCore::VIDEO_CODEC},
                {"audioCodec", "aac"},
                {"maxStreamingBitrate", "4000000"},
                {"maxHeight", "1080"},
                {"api_key", token},
            }));
    case DownloadQuality::Q720p:
        return server + fmt::format(fmt::runtime(jellyfin::apiStream), item.itemId,
            HTTP::encode_form({
                {"static", "false"},
                {"mediaSourceId", item.itemId},
                {"videoCodec", MPVCore::VIDEO_CODEC},
                {"audioCodec", "aac"},
                {"maxStreamingBitrate", "2000000"},
                {"maxHeight", "720"},
                {"api_key", token},
            }));
    case DownloadQuality::Q480p:
        return server + fmt::format(fmt::runtime(jellyfin::apiStream), item.itemId,
            HTTP::encode_form({
                {"static", "false"},
                {"mediaSourceId", item.itemId},
                {"videoCodec", MPVCore::VIDEO_CODEC},
                {"audioCodec", "aac"},
                {"maxStreamingBitrate", "1000000"},
                {"maxHeight", "480"},
                {"api_key", token},
            }));
    }
    return "";
}

// Must be called with mutex held
void DownloadManager::processQueue() {
    if (this->downloading) return;

    for (auto& item : this->items) {
        if (item.status == DownloadStatus::Queued) {
            this->downloading = true;
            this->doDownload(item);
            this->applySleepPolicy();
            return;
        }
    }
    this->applySleepPolicy();
}

void DownloadManager::applySleepPolicy() {
    // A console that dims and drops to sleep takes the transfer down with it,
    // which is what actually interrupts a long download -- not the user. Claim
    // the same "media is playing" state the video player uses while the queue
    // is busy, and release it when it is done.
    bool busy = this->downloading;
    brls::sync([busy]() {
        brls::Application::getPlatform()->disableScreenDimming(
            busy, "Downloading", AppVersion::getPackageName());
    });
}

// Must be called with mutex held. Copies what it needs, then releases via async.
void DownloadManager::doDownload(DownloadItem& item) {
    item.status = DownloadStatus::Downloading;

    std::string itemId = item.itemId;
    std::string imagePrimaryTag = item.imagePrimaryTag;
    DownloadQuality quality = item.quality;
    std::string url = this->buildDownloadUrl(item);
    std::string itemDir = this->downloadDir() + "/" + itemId;
    bool isDirect = !item.sourceUrl.empty();
    std::string presetFile = item.filePath;
    int64_t expectedTotal = item.totalBytes;

    this->saveIndex();

    auto cancel = std::make_shared<std::atomic_bool>(false);
    this->currentCancel = cancel;

    brls::sync([this, itemId]() { this->statusEvent.fire(itemId, DownloadStatus::Downloading); });

    ThreadPool::instance().submit(
        [this, itemId, imagePrimaryTag, quality, url, itemDir, cancel, isDirect, presetFile,
            expectedTotal](HTTP& s) {
        auto resetQueue = [this, itemId](const std::string& error) {
            brls::sync([this, itemId, error]() {
                {
                    std::lock_guard<std::mutex> lock(this->mutex);
                    for (auto& item : this->items) {
                        if (item.itemId == itemId) {
                            item.status = DownloadStatus::Failed;
                            item.errorMessage = error;
                            break;
                        }
                    }
                    this->downloading = false;
                    this->currentCancel.reset();
                    this->saveIndex();
                }
                this->statusEvent.fire(itemId, DownloadStatus::Failed);
                {
                    std::lock_guard<std::mutex> lock(this->mutex);
                    this->processQueue();
                }
            });
        };

        try {
            if (!fs::exists(itemDir)) fs::create_directories(itemDir);
        } catch (const std::exception& e) {
            brls::Logger::error("Failed to create download dir: {}", e.what());
            resetQueue(e.what());
            return;
        }

        auto& conf = AppConfig::instance();
        // Direct downloads go to a third-party host (the debrid service), so
        // the Jellyfin auth header has no business travelling with them.
        HTTP::Header header;
        if (!isDirect) header = {conf.getAuth(conf.getToken())};

        std::string ext = "mp4";
        if (cancel->load()) {
            resetQueue("Cancelled");
            return;
        }
        if (!isDirect && quality == DownloadQuality::Original) {
            try {
                auto resp = HTTP::get(
                    conf.getUrl() + fmt::format(fmt::runtime(jellyfin::apiUserItem), conf.getUserId(), itemId), header,
                    HTTP::Timeout{});
                if (!resp.empty()) {
                    auto detail = nlohmann::json::parse(resp).get<jellyfin::Detail>();
                    if (!detail.MediaSources.empty()) {
                        auto& path = detail.MediaSources[0].Path;
                        auto dot = path.find_last_of('.');
                        if (dot != std::string::npos) {
                            ext = path.substr(dot + 1);
                            std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
                        }
                    }
                }
            } catch (const std::exception& e) {
                brls::Logger::warning("Failed to fetch item detail for extension: {}", e.what());
            }
        }

        // Direct downloads keep the release filename the addon reported;
        // Jellyfin ones stay on the existing "video.<ext>" scheme.
        std::string fileName = (isDirect && !presetFile.empty()) ? presetFile : "video." + ext;
        std::string filePath = itemDir + "/" + fileName;

        {
            std::lock_guard<std::mutex> lock(this->mutex);
            for (auto& it : this->items) {
                if (it.itemId == itemId) {
                    it.filePath = fileName;
                    break;
                }
            }
            this->saveIndex();
        }

        // Pick up where an interrupted download stopped. Only when the expected
        // size is known: without it there is no way to tell afterwards whether
        // the server honoured the range or quietly sent the whole file again,
        // and appending a full body to a partial one produces a file that looks
        // fine and breaks halfway through.
        int64_t resumeFrom = 0;
        if (isDirect && expectedTotal > 0) {
            std::error_code sizeErr;
            auto onDisk = static_cast<int64_t>(fs::file_size(filePath, sizeErr));
            if (!sizeErr && onDisk > 0 && onDisk < expectedTotal) {
                resumeFrom = onDisk;
                brls::Logger::info("Resuming {} at {} of {} bytes", itemId, resumeFrom, expectedTotal);
            }
        }

        if (!imagePrimaryTag.empty() && !cancel->load()) {
            try {
                std::string thumbUrl = fmt::format("{}/Items/{}/Images/Primary?format=Png&{}", conf.getUrl(), itemId,
                    HTTP::encode_form({{"tag", imagePrimaryTag}, {"maxWidth", "300"}}));
                HTTP::download(thumbUrl, itemDir + "/thumb.png", HTTP::Timeout{});
            } catch (const std::exception& e) {
                fs::remove(itemDir + "/thumb.png");
                brls::Logger::warning("Failed to download thumbnail: {}", e.what());
            }
        }

        auto lastProgress = std::make_shared<std::chrono::steady_clock::time_point>();
        HTTP::Progress::Callback progressCb = [this, itemId, lastProgress, resumeFrom](
                                                  curl_off_t total, curl_off_t now) {
            auto tp = std::chrono::steady_clock::now();
            if (tp - *lastProgress < std::chrono::seconds(1)) return;
            *lastProgress = tp;

            // curl counts a ranged request from zero, so shift both figures by
            // what is already on disk to keep the bar showing the whole file.
            brls::sync([this, itemId, total, now, resumeFrom]() {
                {
                    std::lock_guard<std::mutex> lock(this->mutex);
                    for (auto& item : this->items) {
                        if (item.itemId == itemId) {
                            item.totalBytes = total + resumeFrom;
                            item.downloadedBytes = now + resumeFrom;
                            break;
                        }
                    }
                }
                this->progressEvent.fire(itemId, now + resumeFrom, total + resumeFrom);
            });
        };

        bool cancelled = false;
        bool success = false;
        std::string error;

        try {
            if (resumeFrom == 0) {
#ifdef __SWITCH__
                // FAT32 cannot hold a file of 4 GiB or more, and most releases
                // worth downloading are bigger than that. Create it the way the
                // console stores games: a concatenation file, which the
                // filesystem keeps as numbered parts underneath and presents as
                // one file to anything reading through it -- MPV included. Only
                // the console sees it that way; on a PC the card shows a
                // directory with the parts inside.
                std::error_code rmErr;
                fs::remove_all(filePath, rmErr);  // a leftover plain file blocks creation
                Result rc = fsdevCreateFile(filePath.c_str(), 0, FsCreateOption_BigFile);
                if (R_FAILED(rc))
                    brls::Logger::warning("Concatenation file refused (0x{:x}); writing a plain file", rc);
#endif
            }

            std::ofstream of(
                filePath, resumeFrom > 0 ? (std::ios::binary | std::ios::app) : std::ios::binary);
            if (!of) throw std::runtime_error("Failed to open file for writing");

            HTTP s;
            if (resumeFrom > 0) HTTP::set_option(s, HTTP::Range{resumeFrom, 0});
            HTTP::set_option(s, header, cancel, progressCb);
            s._get(url, &of);
            of.close();

            cancelled = cancel->load();
            if (!cancelled && resumeFrom > 0) {
                // A server that ignores the range answers with the whole file,
                // which just got appended to the part already there. The result
                // is longer than it should be and broken in the middle, so check
                // the length before calling it done. Only resumed transfers are
                // checked: a fresh one is trusted as before, because addons do
                // not always report the size accurately and a good file should
                // not be thrown away over that.
                std::error_code chkErr;
                auto finalSize = static_cast<int64_t>(fs::file_size(filePath, chkErr));
                if (!chkErr && finalSize != expectedTotal) {
                    fs::remove_all(filePath, chkErr);
                    throw std::runtime_error("Resume produced the wrong size; the part file was discarded");
                }
            }
            if (!cancelled) success = true;
        } catch (const std::exception& ex) {
            error = ex.what();
            brls::Logger::error("Download failed: {} - {}", itemId, error);
        }

        brls::sync([this, itemId, fileName, cancelled, success, error]() {
            DownloadStatus finalStatus = DownloadStatus::Failed;

            {
                std::lock_guard<std::mutex> lock(this->mutex);

                if (cancelled) {
                    bool removed = false;
                    for (auto it = this->items.begin(); it != this->items.end(); ++it) {
                        if (it->itemId == itemId) {
                            if (it->errorMessage == "removed") {
                                this->items.erase(it);
                                removed = true;
                            } else {
                                it->status = DownloadStatus::Failed;
                                it->errorMessage = "Cancelled";
                            }
                            break;
                        }
                    }
                    this->saveIndex();
                    if (removed) {
                        std::string dir = this->downloadDir() + "/" + itemId;
                        brls::async([dir]() {
                            try {
                                if (fs::exists(dir)) fs::remove_all(dir);
                            } catch (const std::exception& e) {
                                brls::Logger::error("Failed to remove download dir: {}", e.what());
                            }
                        });
                    }
                } else if (success) {
                    finalStatus = DownloadStatus::Completed;
                    for (auto& item : this->items) {
                        if (item.itemId == itemId) {
                            item.status = DownloadStatus::Completed;
                            item.filePath = fileName;

                            std::string metaPath = this->downloadDir() + "/" + itemId + "/metadata.json";
                            try {
                                nlohmann::json j = item;
                                std::ofstream f(metaPath);
                                f << j.dump(2);
                            } catch (...) {
                            }
                            break;
                        }
                    }
                    this->saveIndex();
                    brls::Logger::info("Download completed: {}", itemId);
                } else {
                    for (auto& item : this->items) {
                        if (item.itemId == itemId) {
                            item.status = DownloadStatus::Failed;
                            item.errorMessage = error;
                            break;
                        }
                    }
                    this->saveIndex();
                }

                this->downloading = false;
                this->currentCancel.reset();
            }

            this->statusEvent.fire(itemId, finalStatus);
            {
                std::lock_guard<std::mutex> lock(this->mutex);
                this->processQueue();
            }
        });
    });
}
