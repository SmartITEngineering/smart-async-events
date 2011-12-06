/*
 *
 * This is a framework for Asynchronous Event processing based on event hub.
 * Copyright (C) 2011  Imran M Yousuf (imyousuf@smartitengineering.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.smartitengineering.events.async.api.impl.hub;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.smartitengineering.events.async.api.UriStorer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author imyousuf
 */
@Singleton
public class FileSystemUriStorer implements UriStorer {

  private final File uriStorage;
  private String nextUri;
  protected final transient Logger logger = LoggerFactory.getLogger(getClass());
  private final Semaphore writingInProgressMutex = new Semaphore(1);
  private final Semaphore setNextUriMutex = new Semaphore(1);

  @Inject
  public FileSystemUriStorer(@Named("pathToFolderOfUriStorer") String pathToFolder,
                             @Named("fileNameOfUriStorer") String fileName) {
    File file = new File(pathToFolder);
    if (!file.exists()) {
      boolean mkdirs = file.mkdirs();
      if (!mkdirs) {
        throw new IllegalArgumentException("pathToFolderOfUriStorer does not exist and could not be created!");
      }
    }
    uriStorage = new File(file, fileName);
    if (uriStorage.exists()) {
      readLineFromFile();
    }
    else {
      storeNextUri("");
    }
  }

  @Override
  public final void storeNextUri(String uri) {
    try {
      writingInProgressMutex.acquire();
      logger.info("New URI being stored");
      if (logger.isDebugEnabled()) {
        logger.debug("URI being stored is " + uri);
      }
      BufferedWriter writer = new BufferedWriter(new FileWriter(uriStorage, false));
      writer.write(uri);
      writer.newLine();
      writer.flush();
      writer.close();
      setNextUri(uri);
    }
    catch (Exception ex) {
      logger.error("Could not write to file!", ex);
      throw new RuntimeException(ex);
    }
    finally {
      writingInProgressMutex.release();
    }
  }

  @Override
  public String getNextUri() {
    if (StringUtils.isBlank(nextUri)) {
      readLineFromFile();
    }
    return StringUtils.isBlank(nextUri) ? null : nextUri;
  }

  protected final void readLineFromFile() {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(uriStorage));
      setNextUri(reader.readLine());
      reader.close();
    }
    catch (Exception ex) {
      logger.warn("Could not read file!", ex);
    }
  }

  protected final void setNextUri(String nextUri) {
    try {
      setNextUriMutex.acquire();
      this.nextUri = nextUri;
    }
    catch (Exception ex) {
      logger.warn("Could not acquire lock!", ex);
    }
    finally {
      setNextUriMutex.release();
    }
  }
}
