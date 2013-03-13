// Copyright (C) 2012  Wordnik, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.

package com.wordnik.system.mongodb

import collection.mutable
import com.mongodb.BasicDBObject
import java.io.IOException

/**
 * User: ramesh
 * Date: 7/16/12
 * Time: 8:06 AM
 */

class OpLogReader extends OplogRecordProcessor {

  val recordTriggers = new mutable.HashSet[BasicDBObject => Unit]

  @throws(classOf[Exception])
  def processRecord(dbo: BasicDBObject) = {
    recordTriggers.foreach(t => t(dbo))
  }

  @throws(classOf[IOException])
  def close(string: String) = {

  }

}
