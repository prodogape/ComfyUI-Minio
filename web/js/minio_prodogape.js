import {app} from "/scripts/app.js"

import { api } from '../../../scripts/api.js'
import { applyTextReplacements } from "../../../scripts/utils.js";

function chainCallback(object, property, callback) {
    if (object == undefined) {
        //This should not happen.
        console.error("Tried to add callback to non-existant object")
        return;
    }
    if (property in object) {
        const callback_orig = object[property]
        object[property] = function () {
            const r = callback_orig.apply(this, arguments);
            callback.apply(this, arguments);
            return r
        };
    } else {
        object[property] = callback;
    }
}

function injectHidden(widget) {
    widget.computeSize = (target_width) => {
        if (widget.hidden) {
            return [0, -4];
        }
        return [target_width, 20];
    };
    widget._type = widget.type
    Object.defineProperty(widget, "type", {
        set: function (value) {
            widget._type = value;
        },
        get: function () {
            if (widget.hidden) {
                return "hidden";
            }
            return widget._type;
        }
    });
}


// app.registerExtension({
//     name: "Comfy.UploadImage",
//     async beforeRegisterNodeDef(nodeType, nodeData, app) {
//         if (nodeData.name === "Load Image From Minio") {
//             nodeData.input.required.upload = ["IMAGEUPLOAD"];
//         }
//     },
// });