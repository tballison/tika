package org.apache.tika.batch;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Class that waits for input on System.in.  If the user enters a keystroke on 
 * System.in, this will send a signal to the FileResourceRunner to shutdown gracefully.
 *
 */
public class CommandLineInterrupter implements IInterrupter {
  
	public IFileProcessorFutureResult call(){
		try{
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
			while (true){
				if (reader.ready()){
					reader.readLine();
					break;
				} else {
					Thread.sleep(1000);
				}
			}
		} catch (InterruptedException e){
		    //canceller was interrupted
		} catch (IOException e){
            //TODO: log?
			//e.printStackTrace();
		}
		return new InterrupterFutureResult();
	}
}