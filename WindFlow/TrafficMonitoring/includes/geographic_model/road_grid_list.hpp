/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#ifndef TRAFFICMONITORING_ROAD_GRID_LIST_HPP
#define TRAFFICMONITORING_ROAD_GRID_LIST_HPP

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>

#include "ogrsf_frmts.h"
#include "ogr_geometry.h"
#include "ogr_feature.h"
#include "polygon.hpp"

using namespace std;

class Road_Grid_List {
private:
    unordered_map<string, vector<OGRFeature*>> grid_list;

public:

    /**
     *  @brief Constructor.
     */
    Road_Grid_List() {}

    /**
     *  @brief Method that reads the shapefile.
     *
     *  Read the shapefile, process the road layer and construct a hash map where a key represents the coordinates of a
     *  central point and a value is a list of features corresponding to that point.
     *  @return 0 if the shapefile has been successfully read and the grid_list map created, -1 if an error occurred
     */
    int read_shapefile(const string& shapefile_path) {
        GDALAllRegister();  // registers all format drivers built into GDAL/OGR

        // open the input OGR data source (in this case the shapefile) and use a vector driver
        GDALDataset *dataset = static_cast<GDALDataset*>(GDALOpenEx(shapefile_path.c_str(), GDAL_OF_VECTOR, nullptr, nullptr, nullptr));
        if (dataset == nullptr) {
            cout << "Failed opening GDAL dataset " << shapefile_path << endl;
            return -1;
        }
        //cout << "Successfully opened GDAL dataset " << dataset->GetDescription() << endl;

        // GDALDataset can potentially have many layers associated with it, in this case we have only one layer "roads"
        OGRLayer *roads_layer = dataset->GetLayerByName("roads");
        roads_layer->ResetReading(); // ensure we are starting at the beginning of the "roads" layer

        // feature definition object associated with the layer contains the definitions of all the fields
        OGRFeatureDefn *feature_def = roads_layer->GetLayerDefn();
        OGRFeature *feature = roads_layer->GetNextFeature();

        // iterate through all the features in the "roads layer (return NULL when no more features are available)
        while (feature != nullptr) {
            // extract the geometry from the feature
            OGRGeometry *geometry = feature->GetGeometryRef();
            if (geometry != nullptr) {
                if (geometry->getGeometryType() == 2) {              // GeometryType LINE, GeometryName LINESTRING
                    OGRLineString *line = geometry->toLineString();
                    int length = line->getNumPoints();

                    unique_ptr<OGRPoint> p1(new OGRPoint());
                    unique_ptr<OGRPoint> p2(new OGRPoint());
                    line->getPoint(0, p1.get());
                    line->getPoint(length - 1, p2.get());
                    double center_x = (p1->getX() + p2->getX()) / 2 * 10;
                    double center_y = (p1->getY() + p2->getY()) / 2 * 10;
                    ostringstream map_ID;
                    map_ID << fixed << setprecision(0) << center_y << "_" << fixed << setprecision(0) << center_x;
                    // cout << "Point p1: <" << p1->getX() << ", " << p1->getY() << ">" << endl;
                    // cout << "Point p2: <" << p2->getX() << ", " << p2->getY() << ">" << endl;
                    // cout << "MapID: " << map_ID.str() << endl;

                    if (grid_list.find(map_ID.str()) == grid_list.end())
                        grid_list.emplace(make_pair(map_ID.str(), vector<OGRFeature*>()));
                    else
                        grid_list.at(map_ID.str()).push_back(feature);
                }
            }
            feature = roads_layer->GetNextFeature();
        }
        OGRFeature::DestroyFeature(feature); // method GetNextFeature() returns a copy of the feature that must be freed
        GDALClose(dataset);
        return 0;
    }

    /**
     *  @brief Method that compute a road ID for each GPS position.
     *
     *  Evaluate if there exists a road IDs corresponding to the GPS coordinates contained in point.
     *  @param point GPS coordinates (longitude and latitude) generated by a vehicle
     *  @return the road ID if a match is found, -1 otherwise
     */
    int fetch_road_ID(OGRPoint point) {
        double map_ID_lon = point.getX() * 10;
        double map_ID_lat = point.getY() * 10;
        ostringstream map_ID;
        map_ID << fixed << setprecision(0) << map_ID_lat << "_" << fixed << setprecision(0) << map_ID_lon;
        // cout << "Point point: <" << point.getX() << ", " << point.getY() << ">" << endl;
        // cout << "MapID: " << map_ID.str() << endl;

        int width = 5;
        int last_min_road_ID = -2;
        double last_min_distance = DBL_MAX;
        int grid_count = 0;
        int road_count = 0;

        for (auto entry : grid_list) {
            grid_count++;
            string key = entry.first;
            // cout << "Grid list entry " << grid_count << " key " << key << " vs " << map_ID.str() << endl;

            if (key == map_ID.str()) {
                for (auto feature : entry.second) { // entry.second is a vector<OGRFeature*> (the road_list)
                    road_count++;
                    // retrieve the attribute field road_id of the feature
                    uint64_t road_ID = feature->GetFieldAsInteger64("osm_id");

                    OGRGeometry* geometry = feature->GetGeometryRef();
                    vector<OGRPoint*> points;
                    if (geometry != nullptr) {
                        // wkbFlatten() macro is used to convert the type for a wkbPoint25D (a point with a z coordinate) into
                        // the base 2D geometry type code (wkbPoint); for each 2D geometry type there is a corresponding 2.5D type code;
                        // since the 2D and 2.5D geometry cases are handled by the same C++ class, this code handles 2D or 3D cases properly
                        /*if(wkbFlatten(geometry->getGeometryType()) == wkbPoint) {   // GeometryType POINT
                            cout << "Geometry type POINT" << endl;
                            OGRPoint* p = geometry->toPoint();
                            points.push_back(p);
                            cout << "Point " << p->getX() << ", " << p->getY() << ">" << endl;
                        } else */

                        if (geometry->getGeometryType() == 2) {              // GeometryType LINE, GeometryName LINESTRING
                            OGRLineString* line = geometry->toLineString();
                            for (int i = 0; i < line->getNumPoints(); i++) {
                                OGRPoint* p = new OGRPoint();
                                line->getPoint(i, p);
                                points.push_back(p);
                            }
                        }
                    }

                    Polygon road(points);
                    if (road.match_to_road_line(point, width, &last_min_distance, road_ID, &last_min_road_ID))
                        return road_ID;

                    for (auto p : points)
                        delete p;
                }
                if (last_min_distance < sqrt((width * width) + (10 * 10)))
                    return last_min_road_ID;
                else
                    return -1;
            }

        }
        return -1;
    }

    /**
     *  Destructor.
     */
    ~Road_Grid_List() {}
};

#endif //TRAFFICMONITORING_ROAD_GRID_LIST_HPP
