package com.netflix.evcservice.service;

import com.google.inject.Singleton;
import java.io.PrintWriter;
import com.netflix.server.base.BaseStatusPage;

/**
* Created by senugula on 03/22/15.
*/
@Singleton
public class StatusPage extends BaseStatusPage {

   private static final long serialVersionUID = 1L;

   @Override
   protected void getDetails(PrintWriter out, boolean htmlize) {
       super.getDetails(out, htmlize);
       // Add any extra status info here
   }
}